/*
Copyright 2024 The Crossplane Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package validate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
)

// CRDSourceType defines the type of CRD source.
type CRDSourceType string

const (
	// CRDSourceTypeGitHub fetches CRDs from a GitHub repository.
	CRDSourceTypeGitHub CRDSourceType = "github"
	// CRDSourceTypeCatalog fetches CRDs from the Datree CRDs catalog.
	CRDSourceTypeCatalog CRDSourceType = "catalog"
	// CRDSourceTypeLocal loads CRDs from a local directory.
	CRDSourceTypeLocal CRDSourceType = "local"
	// CRDSourceTypeCluster fetches CRDs from a Kubernetes cluster.
	CRDSourceTypeCluster CRDSourceType = "cluster"
	// CRDSourceTypeK8sSchemas fetches core K8s JSON schemas and converts to CRD format.
	CRDSourceTypeK8sSchemas CRDSourceType = "k8s-schemas"
)

// CRDSource represents a source for CRD schemas.
type CRDSource struct {
	Type     CRDSourceType
	Location string // URL, path, or repo reference
	Branch   string // For GitHub sources
	Path     string // Path within repo for GitHub sources
}

// ParseCRDSource parses a CRD source string into a CRDSource struct.
// Supported formats:
//   - github:org/repo:branch:path (e.g., github:crossplane-contrib/provider-helm:main:package/crds)
//   - local:/path/to/crds
//   - catalog:https://url (e.g., catalog:https://raw.githubusercontent.com/datreeio/CRDs-catalog/main)
//   - cluster (uses current kubeconfig)
func ParseCRDSource(source string) (CRDSource, error) {
	if source == "cluster" {
		return CRDSource{Type: CRDSourceTypeCluster}, nil
	}

	parts := strings.SplitN(source, ":", 2)
	if len(parts) < 2 {
		return CRDSource{}, fmt.Errorf("invalid source format: %s (expected type:location)", source)
	}

	sourceType := parts[0]
	switch sourceType {
	case "github":
		// github:org/repo:branch:path
		rest := parts[1]
		githubParts := strings.SplitN(rest, ":", 3)
		if len(githubParts) < 3 {
			return CRDSource{}, fmt.Errorf("invalid github source: %s (expected github:org/repo:branch:path)", source)
		}
		return CRDSource{
			Type:     CRDSourceTypeGitHub,
			Location: githubParts[0],
			Branch:   githubParts[1],
			Path:     githubParts[2],
		}, nil

	case "local":
		return CRDSource{
			Type:     CRDSourceTypeLocal,
			Location: parts[1],
		}, nil

	case "catalog":
		return CRDSource{
			Type:     CRDSourceTypeCatalog,
			Location: parts[1],
		}, nil

	case "k8s":
		// k8s:v1.29.0 -> fetch from yannh/kubernetes-json-schema
		return CRDSource{
			Type:     CRDSourceTypeK8sSchemas,
			Location: parts[1], // K8s version like "v1.29.0"
		}, nil

	default:
		return CRDSource{}, fmt.Errorf("unknown source type: %s (supported: github, local, catalog, cluster, k8s)", sourceType)
	}
}

// CoreK8sTypes maps GVK strings to their group/version/kind for fetching from kubernetes-json-schema.
// These are Kubernetes built-in types that don't have CRDs but can be validated using JSON schemas.
var CoreK8sTypes = map[string]struct {
	Group   string
	Version string
	Kind    string
}{
	"v1, Kind=Secret":                                                          {Group: "", Version: "v1", Kind: "Secret"},
	"v1, Kind=ConfigMap":                                                       {Group: "", Version: "v1", Kind: "ConfigMap"},
	"v1, Kind=Namespace":                                                       {Group: "", Version: "v1", Kind: "Namespace"},
	"v1, Kind=Service":                                                         {Group: "", Version: "v1", Kind: "Service"},
	"v1, Kind=ServiceAccount":                                                  {Group: "", Version: "v1", Kind: "ServiceAccount"},
	"v1, Kind=PersistentVolumeClaim":                                           {Group: "", Version: "v1", Kind: "PersistentVolumeClaim"},
	"rbac.authorization.k8s.io/v1, Kind=Role":                                  {Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "Role"},
	"rbac.authorization.k8s.io/v1, Kind=RoleBinding":                           {Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "RoleBinding"},
	"rbac.authorization.k8s.io/v1, Kind=ClusterRole":                           {Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"},
	"rbac.authorization.k8s.io/v1, Kind=ClusterRoleBinding":                    {Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleBinding"},
	"storage.k8s.io/v1, Kind=StorageClass":                                     {Group: "storage.k8s.io", Version: "v1", Kind: "StorageClass"},
	"scheduling.k8s.io/v1, Kind=PriorityClass":                                 {Group: "scheduling.k8s.io", Version: "v1", Kind: "PriorityClass"},
	"admissionregistration.k8s.io/v1, Kind=MutatingWebhookConfiguration":       {Group: "admissionregistration.k8s.io", Version: "v1", Kind: "MutatingWebhookConfiguration"},
	"admissionregistration.k8s.io/v1, Kind=ValidatingWebhookConfiguration":     {Group: "admissionregistration.k8s.io", Version: "v1", Kind: "ValidatingWebhookConfiguration"},
	"networking.k8s.io/v1, Kind=Ingress":                                       {Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"},
	"networking.k8s.io/v1, Kind=NetworkPolicy":                                 {Group: "networking.k8s.io", Version: "v1", Kind: "NetworkPolicy"},
	"networking.k8s.io/v1, Kind=IngressClass":                                  {Group: "networking.k8s.io", Version: "v1", Kind: "IngressClass"},
	"apps/v1, Kind=Deployment":                                                 {Group: "apps", Version: "v1", Kind: "Deployment"},
	"apps/v1, Kind=StatefulSet":                                                {Group: "apps", Version: "v1", Kind: "StatefulSet"},
	"apps/v1, Kind=DaemonSet":                                                  {Group: "apps", Version: "v1", Kind: "DaemonSet"},
	"apps/v1, Kind=ReplicaSet":                                                 {Group: "apps", Version: "v1", Kind: "ReplicaSet"},
	"batch/v1, Kind=Job":                                                       {Group: "batch", Version: "v1", Kind: "Job"},
	"batch/v1, Kind=CronJob":                                                   {Group: "batch", Version: "v1", Kind: "CronJob"},
	"policy/v1, Kind=PodDisruptionBudget":                                      {Group: "policy", Version: "v1", Kind: "PodDisruptionBudget"},
	"autoscaling/v1, Kind=HorizontalPodAutoscaler":                             {Group: "autoscaling", Version: "v1", Kind: "HorizontalPodAutoscaler"},
	"autoscaling/v2, Kind=HorizontalPodAutoscaler":                             {Group: "autoscaling", Version: "v2", Kind: "HorizontalPodAutoscaler"},
}

// CRDSourceFetcher fetches CRDs from various sources.
type CRDSourceFetcher struct {
	cacheDir    string
	httpClient  *http.Client
	writer      io.Writer
	parallelism int
}

// NewCRDSourceFetcher creates a new CRDSourceFetcher.
func NewCRDSourceFetcher(cacheDir string, w io.Writer) *CRDSourceFetcher {
	return &CRDSourceFetcher{
		cacheDir: cacheDir,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		writer:      w,
		parallelism: 10, // default
	}
}

// SetParallelism sets the number of parallel fetch operations.
func (f *CRDSourceFetcher) SetParallelism(n int) {
	if n > 0 {
		f.parallelism = n
	}
}

// CleanCache removes all cached CRDs.
func (f *CRDSourceFetcher) CleanCache() error {
	cacheDir := filepath.Join(f.cacheDir, "crd-sources")
	if _, err := os.Stat(cacheDir); os.IsNotExist(err) {
		return nil // Nothing to clean
	}
	return os.RemoveAll(cacheDir)
}

// sourceResult holds the result of fetching from a single source.
type sourceResult struct {
	source CRDSource
	crds   []*extv1.CustomResourceDefinition
	err    error
}

// PrefetchAllFromSources downloads ALL CRDs from all sources in parallel.
// This is useful for pre-populating the cache in Docker images.
// Returns all CRDs found and any errors encountered (non-fatal - continues on error).
func (f *CRDSourceFetcher) PrefetchAllFromSources(ctx context.Context, sources []CRDSource) ([]*extv1.CustomResourceDefinition, []error) {
	if _, err := fmt.Fprintf(f.writer, "\n=== Prefetching ALL CRDs from %d sources (parallel=%d) ===\n\n", len(sources), f.parallelism); err != nil {
		return nil, []error{errors.Wrap(err, "cannot write output")}
	}

	// Create a channel for results and a semaphore for parallelism
	results := make(chan sourceResult, len(sources))
	sem := make(chan struct{}, f.parallelism)

	var wg sync.WaitGroup

	// Launch parallel fetches
	for _, source := range sources {
		wg.Add(1)
		go func(src CRDSource) {
			defer wg.Done()

			// Acquire semaphore
			sem <- struct{}{}
			defer func() { <-sem }()

			crds, err := f.prefetchAllFromSource(ctx, src)
			results <- sourceResult{source: src, crds: crds, err: err}
		}(source)
	}

	// Wait for all fetches to complete and close results channel
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var allCRDs []*extv1.CustomResourceDefinition
	var allErrors []error
	successCount := 0
	crdCount := 0

	for result := range results {
		if result.err != nil {
			allErrors = append(allErrors, fmt.Errorf("%s: %w", result.source.Location, result.err))
			if _, err := fmt.Fprintf(f.writer, "    ❌ %s: %v\n", result.source.Location, result.err); err != nil {
				allErrors = append(allErrors, errors.Wrap(err, "cannot write output"))
			}
		} else {
			allCRDs = append(allCRDs, result.crds...)
			successCount++
			crdCount += len(result.crds)
			if _, err := fmt.Fprintf(f.writer, "    ✅ %s: %d CRDs\n", result.source.Location, len(result.crds)); err != nil {
				allErrors = append(allErrors, errors.Wrap(err, "cannot write output"))
			}
		}
	}

	if _, err := fmt.Fprintf(f.writer, "\n[✓] Prefetched %d CRDs from %d/%d sources\n", crdCount, successCount, len(sources)); err != nil {
		allErrors = append(allErrors, errors.Wrap(err, "cannot write output"))
	}

	return allCRDs, allErrors
}

// prefetchAllFromSource fetches ALL CRDs from a single source (not just required ones).
func (f *CRDSourceFetcher) prefetchAllFromSource(ctx context.Context, source CRDSource) ([]*extv1.CustomResourceDefinition, error) {
	switch source.Type {
	case CRDSourceTypeGitHub:
		return f.prefetchAllFromGitHub(ctx, source)
	case CRDSourceTypeLocal:
		return f.prefetchAllFromLocal(source)
	case CRDSourceTypeK8sSchemas:
		return f.prefetchAllK8sSchemas(ctx, source)
	default:
		// For other types (catalog, cluster), we can't enumerate all CRDs
		return nil, fmt.Errorf("prefetch-all not supported for source type: %s", source.Type)
	}
}

// prefetchAllFromGitHub downloads ALL CRDs from a GitHub repository using the GitHub API.
func (f *CRDSourceFetcher) prefetchAllFromGitHub(ctx context.Context, source CRDSource) ([]*extv1.CustomResourceDefinition, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("github-%s-%s", strings.ReplaceAll(source.Location, "/", "-"), source.Branch)
	cachePath := filepath.Join(f.cacheDir, "crd-sources", cacheKey)

	// If cache exists, load from it
	if info, err := os.Stat(cachePath); err == nil && info.IsDir() {
		return f.loadAllFromCache(cachePath)
	}

	// Use GitHub API to list directory contents
	apiURL := fmt.Sprintf("https://api.github.com/repos/%s/contents/%s?ref=%s",
		source.Location, source.Path, source.Branch)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API returned HTTP %d for %s", resp.StatusCode, apiURL)
	}

	var files []struct {
		Name        string `json:"name"`
		DownloadURL string `json:"download_url"`
		Type        string `json:"type"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return nil, err
	}

	// Filter YAML files
	var yamlFiles []struct {
		Name        string
		DownloadURL string
	}
	for _, file := range files {
		if file.Type == "file" && (strings.HasSuffix(file.Name, ".yaml") || strings.HasSuffix(file.Name, ".yml")) {
			yamlFiles = append(yamlFiles, struct {
				Name        string
				DownloadURL string
			}{Name: file.Name, DownloadURL: file.DownloadURL})
		}
	}

	// Download CRDs in parallel with error collection
	type crdResult struct {
		crd *extv1.CustomResourceDefinition
		err error
		url string
	}

	crdResults := make(chan crdResult, len(yamlFiles))
	sem := make(chan struct{}, f.parallelism)
	var wg sync.WaitGroup

	for _, file := range yamlFiles {
		wg.Add(1)
		go func(name, downloadURL string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			crd, err := f.fetchCRDFromURL(ctx, downloadURL)
			if err != nil {
				crdResults <- crdResult{err: err, url: downloadURL}
				return
			}

			// Only save valid CRDs
			if crd != nil && crd.Kind == "CustomResourceDefinition" {
				f.saveCRDToCache(cachePath, name, crd)
				crdResults <- crdResult{crd: crd}
			} else {
				crdResults <- crdResult{} // Not a CRD, skip silently
			}
		}(file.Name, file.DownloadURL)
	}

	go func() {
		wg.Wait()
		close(crdResults)
	}()

	// Collect results
	var crds []*extv1.CustomResourceDefinition
	var fetchErrors []string
	for result := range crdResults {
		if result.err != nil {
			fetchErrors = append(fetchErrors, fmt.Sprintf("%s: %v", result.url, result.err))
		} else if result.crd != nil {
			crds = append(crds, result.crd)
		}
	}

	// Report errors but don't fail (we still got some CRDs)
	if len(fetchErrors) > 0 && len(fetchErrors) < len(yamlFiles) {
		// Only warn if some files failed, not all
		// If all failed, the caller will see 0 CRDs
	}

	return crds, nil
}

// prefetchAllFromLocal loads ALL CRDs from a local directory.
func (f *CRDSourceFetcher) prefetchAllFromLocal(source CRDSource) ([]*extv1.CustomResourceDefinition, error) {
	var crds []*extv1.CustomResourceDefinition

	err := filepath.Walk(source.Location, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") {
			return nil
		}

		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil // Skip unreadable files
		}

		var crd extv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			return nil // Skip non-CRD files
		}

		if crd.Kind == "CustomResourceDefinition" {
			crds = append(crds, &crd)
		}

		return nil
	})

	return crds, err
}

// prefetchAllK8sSchemas downloads ALL known K8s core type schemas.
func (f *CRDSourceFetcher) prefetchAllK8sSchemas(ctx context.Context, source CRDSource) ([]*extv1.CustomResourceDefinition, error) {
	k8sVersion := source.Location
	if k8sVersion == "" {
		k8sVersion = "v1.29.0"
	}

	// Download all known K8s types in parallel
	type schemaResult struct {
		crd *extv1.CustomResourceDefinition
		err error
		gvk string
	}

	results := make(chan schemaResult, len(CoreK8sTypes))
	sem := make(chan struct{}, f.parallelism)
	var wg sync.WaitGroup

	for gvk, coreType := range CoreK8sTypes {
		wg.Add(1)
		go func(gvkStr string, ct struct {
			Group   string
			Version string
			Kind    string
		}) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			kindLower := strings.ToLower(ct.Kind)
			url := fmt.Sprintf("https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/%s/%s.json",
				k8sVersion, kindLower)

			crd, err := f.fetchJSONSchemaAsCRD(ctx, url, ct.Group, ct.Version, ct.Kind)
			results <- schemaResult{crd: crd, err: err, gvk: gvkStr}
		}(gvk, coreType)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var crds []*extv1.CustomResourceDefinition
	for result := range results {
		if result.err == nil && result.crd != nil {
			crds = append(crds, result.crd)
		}
		// Silently skip errors for individual schemas
	}

	return crds, nil
}

// loadAllFromCache loads ALL CRDs from a cache directory (for prefetch).
func (f *CRDSourceFetcher) loadAllFromCache(cachePath string) ([]*extv1.CustomResourceDefinition, error) {
	var crds []*extv1.CustomResourceDefinition

	err := filepath.Walk(cachePath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".yaml") {
			return nil
		}

		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil
		}

		var crd extv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			return nil
		}

		if crd.Kind == "CustomResourceDefinition" {
			crds = append(crds, &crd)
		}

		return nil
	})

	return crds, err
}

// FetchFromSources fetches CRDs from multiple sources for the required GVKs.
// Uses parallel fetching to speed up the process.
func (f *CRDSourceFetcher) FetchFromSources(ctx context.Context, sources []CRDSource, requiredGVKs map[string]bool) ([]*extv1.CustomResourceDefinition, error) {
	if _, err := fmt.Fprintf(f.writer, "\n=== CRD Source Discovery ===\n"); err != nil {
		return nil, errors.Wrap(err, "cannot write output")
	}
	if _, err := fmt.Fprintf(f.writer, "Looking for %d required CRDs from %d sources (parallel=%d)...\n\n", len(requiredGVKs), len(sources), f.parallelism); err != nil {
		return nil, errors.Wrap(err, "cannot write output")
	}

	// Use parallel fetching for all sources
	type fetchResult struct {
		source CRDSource
		crds   []*extv1.CustomResourceDefinition
		err    error
	}

	results := make(chan fetchResult, len(sources))
	sem := make(chan struct{}, f.parallelism)
	var wg sync.WaitGroup

	// Create a copy of requiredGVKs for thread-safe checking
	// Each goroutine will work with its own foundGVKs map
	for _, source := range sources {
		wg.Add(1)
		go func(src CRDSource) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			// Each goroutine gets its own foundGVKs map for thread safety
			localFoundGVKs := make(map[string]bool)
			crds, err := f.fetchFromSource(ctx, src, requiredGVKs, localFoundGVKs)
			results <- fetchResult{source: src, crds: crds, err: err}
		}(source)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results and merge found GVKs
	var allCRDs []*extv1.CustomResourceDefinition
	foundGVKs := make(map[string]bool)
	var fetchErrors []string

	for result := range results {
		if result.err != nil {
			fetchErrors = append(fetchErrors, fmt.Sprintf("%s: %v", result.source.Location, result.err))
			if _, err := fmt.Fprintf(f.writer, "    ⚠️  %s: %v\n", result.source.Location, result.err); err != nil {
				return nil, errors.Wrap(err, "cannot write output")
			}
		} else if len(result.crds) > 0 {
			// Deduplicate CRDs based on GVK
			for _, crd := range result.crds {
				for _, version := range crd.Spec.Versions {
					gvk := fmt.Sprintf("%s/%s, Kind=%s", crd.Spec.Group, version.Name, crd.Spec.Names.Kind)
					if !foundGVKs[gvk] {
						foundGVKs[gvk] = true
						allCRDs = append(allCRDs, crd)
					}
				}
			}
			if _, err := fmt.Fprintf(f.writer, "    ✅ %s: %d CRDs\n", result.source.Location, len(result.crds)); err != nil {
				return nil, errors.Wrap(err, "cannot write output")
			}
		}
	}

	// Report missing GVKs
	var missingGVKs []string
	for gvk := range requiredGVKs {
		if !foundGVKs[gvk] {
			missingGVKs = append(missingGVKs, gvk)
		}
	}

	if len(missingGVKs) > 0 {
		if _, err := fmt.Fprintf(f.writer, "\n[!] %d required CRDs not found in any source:\n", len(missingGVKs)); err != nil {
			return nil, errors.Wrap(err, "cannot write output")
		}
		for _, gvk := range missingGVKs {
			if _, err := fmt.Fprintf(f.writer, "    ❌ %s\n", gvk); err != nil {
				return nil, errors.Wrap(err, "cannot write output")
			}
		}
		if _, err := fmt.Fprintf(f.writer, "\n"); err != nil {
			return nil, errors.Wrap(err, "cannot write output")
		}
	}

	// Report found CRDs
	if len(foundGVKs) > 0 {
		if _, err := fmt.Fprintf(f.writer, "[✓] Found %d/%d required CRDs from sources\n", len(foundGVKs), len(requiredGVKs)); err != nil {
			return nil, errors.Wrap(err, "cannot write output")
		}
	}

	return allCRDs, nil
}

// GetMissingGVKs returns GVKs that were not found in any source.
func (f *CRDSourceFetcher) GetMissingGVKs(requiredGVKs, foundGVKs map[string]bool) []string {
	var missing []string
	for gvk := range requiredGVKs {
		if !foundGVKs[gvk] {
			missing = append(missing, gvk)
		}
	}
	return missing
}

func (f *CRDSourceFetcher) fetchFromSource(ctx context.Context, source CRDSource, requiredGVKs, foundGVKs map[string]bool) ([]*extv1.CustomResourceDefinition, error) {
	switch source.Type {
	case CRDSourceTypeGitHub:
		return f.fetchFromGitHub(ctx, source, requiredGVKs, foundGVKs)
	case CRDSourceTypeCatalog:
		return f.fetchFromCatalog(ctx, source, requiredGVKs, foundGVKs)
	case CRDSourceTypeLocal:
		return f.fetchFromLocal(source, requiredGVKs, foundGVKs)
	case CRDSourceTypeK8sSchemas:
		return f.fetchFromK8sSchemas(ctx, source, requiredGVKs, foundGVKs)
	default:
		return nil, fmt.Errorf("unknown source type: %s", source.Type)
	}
}

// fetchFromGitHub fetches CRDs from a GitHub repository.
func (f *CRDSourceFetcher) fetchFromGitHub(ctx context.Context, source CRDSource, requiredGVKs, foundGVKs map[string]bool) ([]*extv1.CustomResourceDefinition, error) {
	// Check cache first
	cacheKey := fmt.Sprintf("github-%s-%s", strings.ReplaceAll(source.Location, "/", "-"), source.Branch)
	cachePath := filepath.Join(f.cacheDir, "crd-sources", cacheKey)

	if crds, ok := f.loadFromCache(cachePath, requiredGVKs, foundGVKs); ok {
		if _, err := fmt.Fprintf(f.writer, "Loaded CRDs from cache: %s\n", source.Location); err != nil {
			return nil, errors.Wrap(err, "cannot write output")
		}
		return crds, nil
	}

	if _, err := fmt.Fprintf(f.writer, "Fetching CRDs from GitHub: %s...\n", source.Location); err != nil {
		return nil, errors.Wrap(err, "cannot write output")
	}

	// For now, use raw file URLs for each CRD we need
	// This is more efficient than downloading the entire repo
	// Alternative: download archive from https://github.com/{repo}/archive/refs/heads/{branch}.tar.gz
	var crds []*extv1.CustomResourceDefinition

	for gvk := range requiredGVKs {
		if foundGVKs[gvk] {
			continue
		}

		// Parse GVK: "group/version, Kind=Kind"
		parts := strings.Split(gvk, ", Kind=")
		if len(parts) != 2 {
			continue
		}
		groupVersion := parts[0]
		kind := parts[1]

		gvParts := strings.Split(groupVersion, "/")
		if len(gvParts) != 2 {
			continue
		}
		group := gvParts[0]
		// version := gvParts[1]

		// Try to find the CRD file
		// Upbound providers use: group_kinds.yaml (plural)
		kindLower := strings.ToLower(kind)
		possibleNames := []string{
			fmt.Sprintf("%s_%ss.yaml", group, kindLower),          // e.g., ec2.aws.upbound.io_subnets.yaml
			fmt.Sprintf("%s_%s.yaml", group, kindLower),           // e.g., nats.deinstapel.de_natsaccount.yaml
			fmt.Sprintf("%s_%sies.yaml", group, kindLower[:len(kindLower)-1]), // e.g., policies -> policy
		}

		for _, name := range possibleNames {
			rawURL := fmt.Sprintf("https://raw.githubusercontent.com/%s/%s/%s/%s", 
				source.Location, source.Branch, source.Path, name)

			crd, err := f.fetchCRDFromURL(ctx, rawURL)
			if err == nil && crd != nil {
				// Verify this is the right CRD
				if crd.Spec.Group == group && crd.Spec.Names.Kind == kind {
					crds = append(crds, crd)
					foundGVKs[gvk] = true
					
					// Cache the CRD
					f.saveCRDToCache(cachePath, name, crd)
					break
				}
			}
		}
	}

	return crds, nil
}

// fetchFromCatalog fetches CRDs from the Datree CRDs catalog.
func (f *CRDSourceFetcher) fetchFromCatalog(ctx context.Context, source CRDSource, requiredGVKs, foundGVKs map[string]bool) ([]*extv1.CustomResourceDefinition, error) {
	if _, err := fmt.Fprintf(f.writer, "Fetching CRDs from catalog...\n"); err != nil {
		return nil, errors.Wrap(err, "cannot write output")
	}

	var crds []*extv1.CustomResourceDefinition

	for gvk := range requiredGVKs {
		if foundGVKs[gvk] {
			continue
		}

		// Parse GVK: "group/version, Kind=Kind" or "version, Kind=Kind" (for core K8s)
		parts := strings.SplitN(gvk, ", Kind=", 2)
		if len(parts) != 2 {
			continue
		}
		groupVersion := parts[0]
		kind := parts[1]
		kindLower := strings.ToLower(kind)

		gvParts := strings.Split(groupVersion, "/")

		var urls []string
		if len(gvParts) == 1 {
			// Core K8s type: "v1" -> try kubernetes/{kind}.json
			urls = []string{
				fmt.Sprintf("%s/kubernetes/%s.json", source.Location, kindLower),
			}
		} else if len(gvParts) == 2 {
			group := gvParts[0]
			version := gvParts[1]
			// Standard CRD format: {group}/{kind}_{version}.json
			urls = []string{
				fmt.Sprintf("%s/%s/%s_%s.json", source.Location, group, kindLower, version),
			}
		} else {
			continue
		}

		// Try each URL until one works
		for _, url := range urls {
			crd, err := f.fetchCRDFromURL(ctx, url)
			if err == nil && crd != nil {
				crds = append(crds, crd)
				foundGVKs[gvk] = true
				break
			}
		}
	}

	return crds, nil
}

// fetchFromLocal loads CRDs from a local directory.
func (f *CRDSourceFetcher) fetchFromLocal(source CRDSource, requiredGVKs, foundGVKs map[string]bool) ([]*extv1.CustomResourceDefinition, error) {
	if _, err := fmt.Fprintf(f.writer, "Loading CRDs from local path: %s\n", source.Location); err != nil {
		return nil, errors.Wrap(err, "cannot write output")
	}

	var crds []*extv1.CustomResourceDefinition

	err := filepath.Walk(source.Location, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".yaml") && !strings.HasSuffix(path, ".yml") {
			return nil
		}

		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil // Skip unreadable files
		}

		var crd extv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			return nil // Skip non-CRD files
		}

		if crd.Kind != "CustomResourceDefinition" {
			return nil
		}

		// Check if this CRD is needed
		for _, version := range crd.Spec.Versions {
			gvk := fmt.Sprintf("%s/%s, Kind=%s", crd.Spec.Group, version.Name, crd.Spec.Names.Kind)
			if requiredGVKs[gvk] && !foundGVKs[gvk] {
				crds = append(crds, &crd)
				foundGVKs[gvk] = true
			}
		}

		return nil
	})

	return crds, err
}

func (f *CRDSourceFetcher) fetchCRDFromURL(ctx context.Context, url string) (*extv1.CustomResourceDefinition, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var crd extv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		return nil, err
	}

	return &crd, nil
}

// fetchFromK8sSchemas fetches core K8s JSON schemas from yannh/kubernetes-json-schema
// and converts them to CRD format for validation.
func (f *CRDSourceFetcher) fetchFromK8sSchemas(ctx context.Context, source CRDSource, requiredGVKs, foundGVKs map[string]bool) ([]*extv1.CustomResourceDefinition, error) {
	k8sVersion := source.Location // e.g., "v1.29.0"
	if k8sVersion == "" {
		k8sVersion = "v1.29.0" // default
	}

	if _, err := fmt.Fprintf(f.writer, "Fetching K8s schemas from kubernetes-json-schema (%s)...\n", k8sVersion); err != nil {
		return nil, errors.Wrap(err, "cannot write output")
	}

	var crds []*extv1.CustomResourceDefinition

	for gvk := range requiredGVKs {
		if foundGVKs[gvk] {
			continue
		}

		coreType, ok := CoreK8sTypes[gvk]
		if !ok {
			continue // Not a core K8s type we know about
		}

		// Construct URL: https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/{version}/{kind}.json
		kindLower := strings.ToLower(coreType.Kind)
		url := fmt.Sprintf("https://raw.githubusercontent.com/yannh/kubernetes-json-schema/master/%s/%s.json", k8sVersion, kindLower)

		crd, err := f.fetchJSONSchemaAsCRD(ctx, url, coreType.Group, coreType.Version, coreType.Kind)
		if err == nil && crd != nil {
			crds = append(crds, crd)
			foundGVKs[gvk] = true
		}
	}

	return crds, nil
}

// fetchJSONSchemaAsCRD fetches a JSON schema and converts it to a CRD for validation.
func (f *CRDSourceFetcher) fetchJSONSchemaAsCRD(ctx context.Context, url, group, version, kind string) (*extv1.CustomResourceDefinition, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse JSON schema
	var schema map[string]interface{}
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}

	// Convert JSON schema to OpenAPI v3 schema (they're similar)
	openAPISchema := convertJSONSchemaToOpenAPI(schema)

	// Create a synthetic CRD for validation
	crd := &extv1.CustomResourceDefinition{
		Spec: extv1.CustomResourceDefinitionSpec{
			Group: group,
			Names: extv1.CustomResourceDefinitionNames{
				Kind:     kind,
				Singular: strings.ToLower(kind),
				Plural:   strings.ToLower(kind) + "s",
			},
			Versions: []extv1.CustomResourceDefinitionVersion{
				{
					Name:   version,
					Served: true,
					Schema: &extv1.CustomResourceValidation{
						OpenAPIV3Schema: openAPISchema,
					},
				},
			},
		},
	}

	return crd, nil
}

// convertJSONSchemaToOpenAPI converts a JSON Schema to OpenAPI v3 schema format.
func convertJSONSchemaToOpenAPI(schema map[string]interface{}) *extv1.JSONSchemaProps {
	result := &extv1.JSONSchemaProps{}

	if desc, ok := schema["description"].(string); ok {
		result.Description = desc
	}

	// Handle type (JSON Schema allows arrays like ["string", "null"])
	if t, ok := schema["type"]; ok {
		switch v := t.(type) {
		case string:
			result.Type = v
		case []interface{}:
			// Take the first non-null type
			for _, item := range v {
				if s, ok := item.(string); ok && s != "null" {
					result.Type = s
					break
				}
			}
		}
	}

	if props, ok := schema["properties"].(map[string]interface{}); ok {
		result.Properties = make(map[string]extv1.JSONSchemaProps)
		for name, propSchema := range props {
			if propMap, ok := propSchema.(map[string]interface{}); ok {
				result.Properties[name] = *convertJSONSchemaToOpenAPI(propMap)
			}
		}
	}

	if items, ok := schema["items"].(map[string]interface{}); ok {
		converted := convertJSONSchemaToOpenAPI(items)
		result.Items = &extv1.JSONSchemaPropsOrArray{Schema: converted}
	}

	if additionalProps, ok := schema["additionalProperties"].(map[string]interface{}); ok {
		converted := convertJSONSchemaToOpenAPI(additionalProps)
		result.AdditionalProperties = &extv1.JSONSchemaPropsOrBool{Schema: converted}
	}

	if required, ok := schema["required"].([]interface{}); ok {
		for _, r := range required {
			if s, ok := r.(string); ok {
				result.Required = append(result.Required, s)
			}
		}
	}

	return result
}

func (f *CRDSourceFetcher) loadFromCache(cachePath string, requiredGVKs, foundGVKs map[string]bool) ([]*extv1.CustomResourceDefinition, bool) {
	info, err := os.Stat(cachePath)
	if err != nil || !info.IsDir() {
		return nil, false
	}

	// Cache is valid indefinitely until explicitly cleaned with --clean-crd-cache
	var crds []*extv1.CustomResourceDefinition

	err = filepath.Walk(cachePath, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !strings.HasSuffix(path, ".yaml") {
			return nil
		}

		data, err := os.ReadFile(filepath.Clean(path))
		if err != nil {
			return nil
		}

		var crd extv1.CustomResourceDefinition
		if err := yaml.Unmarshal(data, &crd); err != nil {
			return nil
		}

		// Check if this CRD is needed
		for _, version := range crd.Spec.Versions {
			gvk := fmt.Sprintf("%s/%s, Kind=%s", crd.Spec.Group, version.Name, crd.Spec.Names.Kind)
			if requiredGVKs[gvk] && !foundGVKs[gvk] {
				crds = append(crds, &crd)
				foundGVKs[gvk] = true
			}
		}

		return nil
	})

	if err != nil {
		return nil, false
	}

	return crds, len(crds) > 0
}

func (f *CRDSourceFetcher) saveCRDToCache(cachePath, filename string, crd *extv1.CustomResourceDefinition) error {
	if err := os.MkdirAll(cachePath, 0755); err != nil {
		return err
	}

	data, err := yaml.Marshal(crd)
	if err != nil {
		return err
	}

	return os.WriteFile(filepath.Join(cachePath, filename), data, 0644)
}

// ParseCRDSources parses a list of CRD source specifications.
// Formats supported:
//   - Well-known names: "upjet-aws", "upjet-azure", "nats-operator", "datree-catalog"
//   - GitHub URLs: "https://github.com/crossplane-contrib/provider-upjet-aws"
//   - GitHub URLs with path: "https://github.com/owner/repo#branch:path/to/crds"
//   - Explicit format: "github:owner/repo:branch:path"
//   - Local paths: "local:/path/to/crds" or "/path/to/crds"
//   - Catalog URL: "catalog:https://..."
func ParseCRDSources(specs []string) ([]CRDSource, error) {
	var sources []CRDSource

	for _, spec := range specs {
		source, err := parseSingleSource(spec)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}

	return sources, nil
}

func parseSingleSource(spec string) (CRDSource, error) {
	// Check for cluster source
	if spec == "cluster" {
		return CRDSource{Type: CRDSourceTypeCluster}, nil
	}

	// Check if it's a GitHub URL (https://github.com/...)
	if strings.HasPrefix(spec, "https://github.com/") {
		return parseGitHubURL(spec)
	}

	// Check if it's a local path (starts with / or ./)
	if strings.HasPrefix(spec, "/") || strings.HasPrefix(spec, "./") || strings.HasPrefix(spec, "../") {
		return CRDSource{
			Type:     CRDSourceTypeLocal,
			Location: spec,
		}, nil
	}

	// Parse explicit format: type:location
	parts := strings.SplitN(spec, ":", 2)
	if len(parts) < 2 {
		return CRDSource{}, fmt.Errorf("invalid CRD source: %s\nSupported formats:\n"+
			"  - GitHub URL: https://github.com/owner/repo\n"+
			"  - GitHub URL with path: https://github.com/owner/repo#branch:path/to/crds\n"+
			"  - Explicit: github:owner/repo:branch:path\n"+
			"  - Catalog: catalog:https://raw.githubusercontent.com/datreeio/CRDs-catalog/main\n"+
			"  - K8s core schemas: k8s:v1.29.0\n"+
			"  - Cluster: cluster\n"+
			"  - Local: /path/to/crds or local:/path/to/crds", spec)
	}

	sourceTypeStr := parts[0]
	location := parts[1]

	switch sourceTypeStr {
	case "github":
		// Format: github:owner/repo:branch:path
		githubParts := strings.SplitN(location, ":", 3)
		if len(githubParts) < 1 {
			return CRDSource{}, fmt.Errorf("invalid GitHub source: %s", spec)
		}
		source := CRDSource{
			Type:     CRDSourceTypeGitHub,
			Location: githubParts[0],
			Branch:   "main",
			Path:     "package/crds",
		}
		if len(githubParts) > 1 {
			source.Branch = githubParts[1]
		}
		if len(githubParts) > 2 {
			source.Path = githubParts[2]
		}
		return source, nil

	case "local":
		return CRDSource{
			Type:     CRDSourceTypeLocal,
			Location: location,
		}, nil

	case "catalog":
		return CRDSource{
			Type:     CRDSourceTypeCatalog,
			Location: location,
		}, nil

	case "k8s":
		return CRDSource{
			Type:     CRDSourceTypeK8sSchemas,
			Location: location, // K8s version like "v1.29.0"
		}, nil

	default:
		return CRDSource{}, fmt.Errorf("unknown CRD source type: %s", sourceTypeStr)
	}
}

// parseGitHubURL parses a GitHub URL into a CRDSource.
// Formats:
//   - https://github.com/owner/repo
//   - https://github.com/owner/repo#branch
//   - https://github.com/owner/repo#branch:path/to/crds
func parseGitHubURL(url string) (CRDSource, error) {
	// Remove https://github.com/ prefix
	path := strings.TrimPrefix(url, "https://github.com/")
	
	source := CRDSource{
		Type:   CRDSourceTypeGitHub,
		Branch: "main",
		Path:   "package/crds", // Default for Upbound providers
	}

	// Check for #branch:path suffix
	if idx := strings.Index(path, "#"); idx != -1 {
		extra := path[idx+1:]
		path = path[:idx]

		// Parse branch:path
		if colonIdx := strings.Index(extra, ":"); colonIdx != -1 {
			source.Branch = extra[:colonIdx]
			source.Path = extra[colonIdx+1:]
		} else {
			source.Branch = extra
		}
	}

	// Extract owner/repo
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		return CRDSource{}, fmt.Errorf("invalid GitHub URL: %s (expected https://github.com/owner/repo)", url)
	}
	source.Location = parts[0] + "/" + parts[1]

	// Auto-detect path for known patterns
	if source.Path == "package/crds" {
		// Check if this looks like a kubebuilder project (config/crd/bases)
		if strings.Contains(strings.ToLower(source.Location), "operator") {
			source.Path = "config/crd/bases"
		}
	}

	return source, nil
}
