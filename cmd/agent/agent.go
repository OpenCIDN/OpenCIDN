package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/OpenCIDN/OpenCIDN/internal/pki"
	"github.com/OpenCIDN/OpenCIDN/internal/server"
	"github.com/OpenCIDN/OpenCIDN/internal/signals"
	"github.com/OpenCIDN/OpenCIDN/pkg/blobs"
	"github.com/OpenCIDN/OpenCIDN/pkg/cache"
	"github.com/OpenCIDN/OpenCIDN/pkg/queue/client"
	"github.com/OpenCIDN/OpenCIDN/pkg/signing"
	"github.com/OpenCIDN/OpenCIDN/pkg/token"
	"github.com/OpenCIDN/OpenCIDN/pkg/transport"
	"github.com/gorilla/handlers"
	"github.com/spf13/cobra"
	"github.com/wzshiming/httpseek"
	"github.com/wzshiming/sss"
)

func main() {
	ctx := signals.SetupSignalContext()
	err := NewCommand().ExecuteContext(ctx)
	if err != nil {
		slog.Error("execute failed", "error", err)
		os.Exit(1)
	}
}

type flagpole struct {
	BigStorageURL  string
	BigStorageSize int

	StorageURL    string
	RedirectLinks string
	LinkExpires   time.Duration
	SignLink      bool

	Userpass      []string
	Retry         int
	RetryInterval time.Duration

	Behind         bool
	Address        string
	AcmeHosts      []string
	AcmeCacheDir   string
	CertFile       string
	PrivateKeyFile string

	TokenPublicKeyFile string
	TokenURL           string

	BlobNoRedirectSize             int
	BlobNoRedirectMaxSizePerSecond int
	BlobCacheDuration              time.Duration
	ForceBlobNoRedirect            bool

	Concurrency int

	QueueURL   string
	QueueToken string
}

func NewCommand() *cobra.Command {
	flags := &flagpole{
		Address:           ":18002",
		BlobCacheDuration: time.Hour,
		Concurrency:       10,
		SignLink:          true,
		LinkExpires:       1 * time.Hour,
	}

	cmd := &cobra.Command{
		Use:   "blobs",
		Short: "Blobs",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runE(cmd.Context(), flags)
		},
	}

	cmd.Flags().StringVar(&flags.StorageURL, "storage-url", flags.StorageURL, "Storage driver url")
	cmd.Flags().StringVar(&flags.BigStorageURL, "big-storage-url", flags.BigStorageURL, "Big storage driver url")
	cmd.Flags().IntVar(&flags.BigStorageSize, "big-storage-size", flags.BigStorageSize, "Big storage size")
	cmd.Flags().StringVar(&flags.RedirectLinks, "redirect-links", flags.RedirectLinks, "Redirect links")
	cmd.Flags().DurationVar(&flags.LinkExpires, "link-expires", flags.LinkExpires, "Link expires")
	cmd.Flags().BoolVar(&flags.SignLink, "sign-link", flags.SignLink, "Sign Link")

	cmd.Flags().StringSliceVarP(&flags.Userpass, "user", "u", flags.Userpass, "host and username and password -u user:pwd@host")
	cmd.Flags().IntVar(&flags.Retry, "retry", flags.Retry, "Retry")
	cmd.Flags().DurationVar(&flags.RetryInterval, "retry-interval", flags.RetryInterval, "Retry interval")

	cmd.Flags().BoolVar(&flags.Behind, "behind", flags.Behind, "Behind")
	cmd.Flags().StringVar(&flags.Address, "address", flags.Address, "Address")
	cmd.Flags().StringSliceVar(&flags.AcmeHosts, "acme-hosts", flags.AcmeHosts, "Acme hosts")
	cmd.Flags().StringVar(&flags.AcmeCacheDir, "acme-cache-dir", flags.AcmeCacheDir, "Acme cache dir")
	cmd.Flags().StringVar(&flags.CertFile, "cert-file", flags.CertFile, "Cert file")
	cmd.Flags().StringVar(&flags.PrivateKeyFile, "private-key-file", flags.PrivateKeyFile, "Private key file")

	cmd.Flags().StringVar(&flags.TokenPublicKeyFile, "token-public-key-file", flags.TokenPublicKeyFile, "Token public key file")
	cmd.Flags().StringVar(&flags.TokenURL, "token-url", flags.TokenURL, "Token url")

	cmd.Flags().IntVar(&flags.BlobNoRedirectSize, "blob-no-redirect-size", flags.BlobNoRedirectSize, "Less than or equal to no redirect")
	cmd.Flags().IntVar(&flags.BlobNoRedirectMaxSizePerSecond, "blob-no-redirect-max-size-per-second", flags.BlobNoRedirectMaxSizePerSecond, "Maximum size per second for no redirect")
	cmd.Flags().DurationVar(&flags.BlobCacheDuration, "blob-cache-duration", flags.BlobCacheDuration, "Blob cache duration")
	cmd.Flags().BoolVar(&flags.ForceBlobNoRedirect, "force-blob-no-redirect", flags.ForceBlobNoRedirect, "Force blob no redirect")

	cmd.Flags().IntVar(&flags.Concurrency, "concurrency", flags.Concurrency, "Concurrency to source")

	cmd.Flags().StringVar(&flags.QueueToken, "queue-token", flags.QueueToken, "Queue token")
	cmd.Flags().StringVar(&flags.QueueURL, "queue-url", flags.QueueURL, "Queue URL")

	return cmd
}

func runE(ctx context.Context, flags *flagpole) error {
	mux := http.NewServeMux()

	blobsOpts := []blobs.Option{}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	cacheOpts := []cache.Option{
		cache.WithSignLink(flags.SignLink),
	}

	sd, err := sss.NewSSS(sss.WithURL(flags.StorageURL))
	if err != nil {
		return fmt.Errorf("create storage driver failed: %w", err)
	}
	cacheOpts = append(cacheOpts, cache.WithStorageDriver(sd))
	if flags.LinkExpires > 0 {
		cacheOpts = append(cacheOpts, cache.WithLinkExpires(flags.LinkExpires))
	}

	if flags.RedirectLinks != "" {
		u, err := url.Parse(flags.RedirectLinks)
		if err != nil {
			return fmt.Errorf("parse redirect links failed: %w", err)
		}
		cacheOpts = append(cacheOpts, cache.WithRedirectLinks(u))
	}

	sdcache, err := cache.NewCache(cacheOpts...)
	if err != nil {
		return fmt.Errorf("create cache failed: %w", err)
	}

	blobsOpts = append(blobsOpts,
		blobs.WithCache(sdcache),
		blobs.WithLogger(logger),
		blobs.WithBlobNoRedirectSize(flags.BlobNoRedirectSize),
		blobs.WithBlobNoRedirectMaxSizePerSecond(flags.BlobNoRedirectMaxSizePerSecond),
		blobs.WithBlobCacheDuration(flags.BlobCacheDuration),
		blobs.WithForceBlobNoRedirect(flags.ForceBlobNoRedirect),
		blobs.WithConcurrency(flags.Concurrency),
	)

	if flags.BigStorageURL != "" && flags.BigStorageSize > 0 {
		bigCacheOpts := []cache.Option{}
		sd, err := sss.NewSSS(sss.WithURL(flags.BigStorageURL))
		if err != nil {
			return fmt.Errorf("create storage driver failed: %w", err)
		}
		bigCacheOpts = append(bigCacheOpts,
			cache.WithSignLink(flags.SignLink),
			cache.WithStorageDriver(sd),
		)
		if flags.LinkExpires > 0 {
			bigCacheOpts = append(bigCacheOpts, cache.WithLinkExpires(flags.LinkExpires))
		}
		bigsdcache, err := cache.NewCache(bigCacheOpts...)
		if err != nil {
			return fmt.Errorf("create cache failed: %w", err)
		}
		blobsOpts = append(blobsOpts, blobs.WithBigCache(bigsdcache, flags.BigStorageSize))
	}

	if flags.QueueURL != "" {
		queueClient := client.NewMessageClient(http.DefaultClient, flags.QueueURL, flags.QueueToken)
		blobsOpts = append(blobsOpts, blobs.WithQueueClient(queueClient))
	}

	if flags.TokenPublicKeyFile != "" {
		publicKeyData, err := os.ReadFile(flags.TokenPublicKeyFile)
		if err != nil {
			return fmt.Errorf("failed to read token public key file: %w", err)
		}
		publicKey, err := pki.DecodePublicKey(publicKeyData)
		if err != nil {
			return fmt.Errorf("failed to decode token public key: %w", err)
		}

		authenticator := token.NewAuthenticator(token.NewDecoder(signing.NewVerifier(publicKey)), flags.TokenURL)
		blobsOpts = append(blobsOpts, blobs.WithAuthenticator(authenticator))
	}

	transportOpts := []transport.Option{
		transport.WithUserAndPass(flags.Userpass),
		transport.WithLogger(logger),
	}

	tp, err := transport.NewTransport(transportOpts...)
	if err != nil {
		return fmt.Errorf("create clientset failed: %w", err)
	}

	if flags.RetryInterval > 0 {
		tp = httpseek.NewMustReaderTransport(tp, func(request *http.Request, retry int, err error) error {
			if errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if flags.Retry > 0 && retry >= flags.Retry {
				return err
			}
			if logger != nil {
				logger.Warn("Retry", "url", request.URL, "retry", retry, "error", err)
			}
			time.Sleep(flags.RetryInterval)
			return nil
		})
	}

	tp = transport.NewLogTransport(tp, logger, time.Minute)

	httpClient := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > 10 {
				return http.ErrUseLastResponse
			}
			s := make([]string, 0, len(via)+1)
			for _, v := range via {
				s = append(s, v.URL.String())
			}

			lastRedirect := req.URL.String()
			s = append(s, lastRedirect)
			logger.Info("redirect", "redirects", s)

			return nil
		},
		Transport: tp,
	}
	blobsOpts = append(blobsOpts, blobs.WithClient(httpClient))

	a, err := blobs.NewBlobs(blobsOpts...)
	if err != nil {
		return fmt.Errorf("create blobs failed: %w", err)
	}

	mux.Handle("/v2/", a)

	var handler http.Handler = mux
	handler = handlers.LoggingHandler(os.Stderr, handler)
	if flags.Behind {
		handler = handlers.ProxyHeaders(handler)
	}

	err = server.Run(ctx, flags.Address, handler, flags.AcmeHosts, flags.AcmeCacheDir, flags.CertFile, flags.PrivateKeyFile)
	if err != nil {
		return fmt.Errorf("failed to run server: %w", err)
	}
	return nil
}
