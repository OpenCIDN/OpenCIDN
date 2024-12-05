package crproxy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/daocloud/crproxy/token"
	"github.com/docker/distribution/registry/api/errcode"
)

func manifestRevisionsCachePath(host, image, tagOrBlob string) string {
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/revisions/sha256", tagOrBlob, "link")
}

func manifestTagCachePath(host, image, tagOrBlob string) string {
	return path.Join("/docker/registry/v2/repositories", host, image, "_manifests/tags", tagOrBlob, "current/link")
}

func (c *CRProxy) cacheManifestResponse(rw http.ResponseWriter, r *http.Request, info *PathInfo, t *token.Token) {
	if c.tryFirstServeCachedManifest(rw, r, info) {
		return
	}

	cli := c.client.GetClientset(info.Host, info.Image)
	resp, err := c.client.DoWithAuth(cli, r, info.Host)
	if err != nil {
		if c.fallbackServeCachedManifest(rw, r, info) {
			return
		}
		c.logger.Error("failed to request", "host", info.Host, "image", info.Image, "error", err)
		errcode.ServeJSON(rw, errcode.ErrorCodeUnknown)
		return
	}
	defer func() {
		resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		if c.fallbackServeCachedManifest(rw, r, info) {
			c.logger.Error("origin manifest response 40x, but hit caches", "host", info.Host, "image", info.Image, "error", err, "response", dumpResponse(resp))
			return
		}
		c.logger.Error("origin manifest response 40x", "host", info.Host, "image", info.Image, "error", err, "response", dumpResponse(resp))
		errcode.ServeJSON(rw, errcode.ErrorCodeDenied)
		return
	}

	if resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError {
		if c.fallbackServeCachedManifest(rw, r, info) {
			c.logger.Error("origin manifest response 4xx, but hit caches", "host", info.Host, "image", info.Image, "error", err, "response", dumpResponse(resp))
			return
		}
		c.logger.Error("origin manifest response 4xx", "host", info.Host, "image", info.Image, "error", err, "response", dumpResponse(resp))
	} else if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusInternalServerError {
		if c.fallbackServeCachedManifest(rw, r, info) {
			c.logger.Error("origin manifest response 5xx, but hit caches", "host", info.Host, "image", info.Image, "error", err, "response", dumpResponse(resp))
			return
		}
		c.logger.Error("origin manifest response 5xx", "host", info.Host, "image", info.Image, "error", err, "response", dumpResponse(resp))
	}

	resp.Header.Del("Docker-Ratelimit-Source")

	header := rw.Header()
	for k, v := range resp.Header {
		key := textproto.CanonicalMIMEHeaderKey(k)
		header[key] = v
	}

	rw.WriteHeader(resp.StatusCode)

	if r.Method == http.MethodHead {
		return
	}

	if resp.StatusCode >= http.StatusOK || resp.StatusCode < http.StatusMultipleChoices {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			c.errorResponse(rw, r, err)
			return
		}

		err = c.cacheManifestContent(context.Background(), info, body)
		if err != nil {
			c.errorResponse(rw, r, err)
			return
		}
		rw.Write(body)
	} else {
		io.Copy(rw, resp.Body)
	}
}

func (c *CRProxy) cacheManifestContent(ctx context.Context, info *PathInfo, content []byte) error {
	h := sha256.New()
	h.Write(content)
	hash := hex.EncodeToString(h.Sum(nil)[:])
	isHash := strings.HasPrefix(info.Manifests, "sha256:")

	if isHash {
		if info.Manifests[7:] != hash {
			return fmt.Errorf("expected hash %s is not same to %s", info.Manifests[7:], hash)
		}
	} else {
		manifestLinkPath := manifestTagCachePath(info.Host, info.Image, info.Manifests)
		err := c.storageDriver.PutContent(ctx, manifestLinkPath, []byte("sha256:"+hash))
		if err != nil {
			return err
		}
		if c.manifestCacheDuration > 0 {
			c.manifestCache.Store(manifestLinkPath, time.Now())
		}
	}

	manifestLinkPath := manifestRevisionsCachePath(info.Host, info.Image, hash)
	err := c.storageDriver.PutContent(ctx, manifestLinkPath, []byte("sha256:"+hash))
	if err != nil {
		return err
	}

	blobCachePath := blobCachePath(hash)
	err = c.storageDriver.PutContent(ctx, blobCachePath, content)
	if err != nil {
		return err
	}
	return nil
}

func (c *CRProxy) tryFirstServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	isHash := strings.HasPrefix(info.Manifests, "sha256:")

	var manifestLinkPath string
	if isHash {
		manifestLinkPath = manifestRevisionsCachePath(info.Host, info.Image, info.Manifests[7:])
	} else {
		manifestLinkPath = manifestTagCachePath(info.Host, info.Image, info.Manifests)
	}

	if !isHash && c.manifestCacheDuration > 0 {
		last, ok := c.manifestCache.Load(manifestLinkPath)
		if !ok {
			return false
		}

		if time.Since(last) > c.manifestCacheDuration {
			return false
		}
	}

	return c.serveCachedManifest(rw, r, manifestLinkPath)
}

func (c *CRProxy) fallbackServeCachedManifest(rw http.ResponseWriter, r *http.Request, info *PathInfo) bool {
	isHash := strings.HasPrefix(info.Manifests, "sha256:")
	if isHash {
		return false
	}
	var manifestLinkPath string
	if isHash {
		manifestLinkPath = manifestRevisionsCachePath(info.Host, info.Image, info.Manifests[7:])
	} else {
		manifestLinkPath = manifestTagCachePath(info.Host, info.Image, info.Manifests)
	}

	return c.serveCachedManifest(rw, r, manifestLinkPath)
}

func (c *CRProxy) serveCachedManifest(rw http.ResponseWriter, r *http.Request, manifestLinkPath string) bool {
	ctx := r.Context()

	digestContent, err := c.storageDriver.GetContent(ctx, manifestLinkPath)
	if err != nil {
		c.logger.Error("Manifest cache missed", "manifestLinkPath", manifestLinkPath, "error", err)
		return false
	}

	digest := string(digestContent)
	blobCachePath := blobCachePath(digest)
	content, err := c.storageDriver.GetContent(ctx, blobCachePath)
	if err != nil {
		c.logger.Error("Manifest blob cache missed", "blobCachePath", blobCachePath, "error", err)
		return false
	}

	mt := struct {
		MediaType string `json:"mediaType"`
	}{}
	err = json.Unmarshal(content, &mt)
	if err != nil {
		c.logger.Error("Manifest blob cache err", "blobCachePath", blobCachePath, "error", err)
		return false
	}
	c.logger.Info("Manifest blob cache hit", "blobCachePath", blobCachePath)
	rw.Header().Set("docker-content-digest", digest)
	rw.Header().Set("Content-Type", mt.MediaType)
	rw.Header().Set("Content-Length", strconv.FormatInt(int64(len(content)), 10))
	if r.Method != http.MethodHead {
		rw.Write(content)
	}

	if c.manifestCacheDuration > 0 {
		c.manifestCache.Store(manifestLinkPath, time.Now())
	}
	return true
}
