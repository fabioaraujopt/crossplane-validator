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

// Package validate implements offline schema validation of Crossplane resources.
package validate

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/alecthomas/kong"
	"github.com/spf13/afero"
	extv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"

	"github.com/crossplane/crossplane-runtime/v2/pkg/errors"
	"github.com/crossplane/crossplane-runtime/v2/pkg/logging"

	"github.com/crossplane/crossplane/v2/cmd/crank/common/load"
	"github.com/crossplane/crossplane/v2/internal/version"
)

// Cmd arguments and flags for render subcommand.
type Cmd struct {
	// Arguments.
	Extensions string `arg:"" help:"Extension sources as a comma-separated list of files, directories, or '-' for standard input."`
	Resources  string `arg:"" help:"Resource sources as a comma-separated list of files, directories, or '-' for standard input."`

	// Flags. Keep them in alphabetical order.
	CacheDir              string `default:"~/.crossplane/cache"                                                       help:"Absolute path to the cache directory where downloaded schemas are stored." predictor:"directory"`
	CleanCache            bool   `help:"Clean the cache directory before downloading package schemas."`
	SkipSuccessResults    bool   `help:"Skip printing success results."`
	CrossplaneImage       string `help:"Specify the Crossplane image to be used for validating the built-in schemas."`
	ErrorOnMissingSchemas bool   `default:"false"                                                                     help:"Return non zero exit code if not all schemas are provided."`

	// New validation flags
	ValidatePatches       bool `default:"true"  help:"Validate that patch fromFieldPath and toFieldPath exist in their respective schemas."`
	DetectUnusedParams    bool `default:"true"  help:"Detect XRD parameters that are defined but never used in composition patches."`
	ValidateStatusChains  bool `default:"true"  help:"Validate status field propagation through composition hierarchies."`
	StrictMode            bool `default:"false" help:"Treat warnings (like unused parameters) as errors."`
	SkipCompositionChecks bool `default:"false" help:"Skip composition-level validations (patch paths, unused params)."`
	ShowTree              bool `default:"false" help:"Show composition tree hierarchy and per-composition analysis."`
	ShowDetails           bool `default:"true"  help:"Show detailed error messages (invalid patches, unused params per composition)."`
	AutoDiscoverProviders bool `default:"false" help:"Automatically discover and download provider schemas based on resources used in compositions."`
	OnlyInvalid           bool `default:"false" help:"Only show invalid/error results, hide all success output."`

	// Cluster-based schema fetching
	UseCluster             bool   `default:"false" help:"Fetch CRD schemas from a live Kubernetes cluster instead of downloading from registry."`
	Kubeconfig             string `help:"Path to kubeconfig file. Uses default kubeconfig if not specified." predictor:"file"`
	KubeContext            string `help:"Kubernetes context to use from kubeconfig."`
	ValidateFunctionInputs bool   `default:"true"  help:"Download function packages and validate pipeline inputs against their schemas."`

	// CRD sources for offline validation
	CRDSources       []string `help:"CRD sources for schema validation. Formats: github:org/repo:branch:path, catalog:https://url, local:/path/to/crds, cluster, or GitHub URLs (https://github.com/owner/repo#branch:path)." sep:","`
	FailOnMissingCRD bool     `default:"false" help:"Fail validation if required CRDs cannot be found in any source."`
	CleanCRDCache    bool     `default:"false" help:"Clean the CRD source cache before fetching. Forces re-download of all CRDs."`
	PrefetchAllCRDs  bool     `name:"prefetch-all-crds" default:"false" help:"Download ALL CRDs from each source, not just those needed for validation. Useful for pre-caching in Docker images."`
	ParallelFetch    int      `name:"parallel-fetch" default:"10"    help:"Number of parallel CRD fetch operations. Higher values speed up downloads but use more connections."`

	fs afero.Fs
}

// Help prints out the help for the validate command.
func (c *Cmd) Help() string {
	return `
This command validates the provided Crossplane resources against the schemas of the provided extensions like XRDs,
CRDs, providers, functions and configurations. The output of the "crossplane render" command can be
piped to this validate command in order to rapidly validate on the outputs of the composition development experience.

If providers or configurations are provided as extensions, they will be downloaded and loaded as CRDs before performing
validation. If the cache directory is not provided, it will default to "~/.crossplane/cache".
Cache directory can be cleaned before downloading schemas by setting the "clean-cache" flag.

All validation is performed offline locally using the Kubernetes API server's validation library, so it does not require
any Crossplane instance or control plane to be running or configured.

COMPREHENSIVE VALIDATION:

This command performs the following validations:

1. SCHEMA VALIDATION (always enabled):
   - Validates resources against CRD/XRD OpenAPI schemas
   - Detects missing required fields
   - Detects unknown fields (typos)
   - Validates field types and enum values
   - Evaluates CEL validation rules

2. PATCH PATH VALIDATION (--validate-patches, default: true):
   - Validates that fromFieldPath exists in the source schema (XR or composed resource)
   - Validates that toFieldPath exists in the target schema
   - Catches typos in patch paths before runtime errors

3. UNUSED PARAMETER DETECTION (--detect-unused-params, default: true):
   - Analyzes XRD parameters and composition patches
   - Reports parameters defined in XRD but never used in any patch
   - Helps identify dead configuration code

4. STRICT MODE (--strict-mode):
   - Treats warnings (unused parameters) as errors
   - Returns non-zero exit code for any issue

5. CRD SOURCES (--crd-sources):
   - Fetch CRDs from multiple sources for complete validation
   - Built-in well-known sources: upjet-aws, upjet-azure, nats-operator, datree-catalog
   - Custom GitHub repos, local directories, or the Datree catalog
   - Caches downloaded CRDs for faster subsequent runs

Examples:

  # Validate all resources in the resources.yaml file against the extensions in the extensions.yaml file
  crossplane beta validate extensions.yaml resources.yaml

  # Validate with strict mode (treat unused params as errors)
  crossplane beta validate extensions.yaml resources.yaml --strict-mode

  # Use offline CRD sources (no cluster access needed)
  # Well-known names are shortcuts for popular repos
  crossplane beta validate extensions.yaml resources.yaml \
    --crd-sources upjet-aws,upjet-azure,nats-operator

  # Use GitHub URLs directly (auto-detects CRD path)
  crossplane beta validate extensions.yaml resources.yaml \
    --crd-sources https://github.com/crossplane-contrib/provider-upjet-aws

  # GitHub URL with custom branch and path
  crossplane beta validate extensions.yaml resources.yaml \
    --crd-sources "https://github.com/my-org/my-crds#main:crds/"

  # Use local CRD directory
  crossplane beta validate extensions.yaml resources.yaml \
    --crd-sources /path/to/my/crds

  # Multiple sources - tries each until CRD is found
  crossplane beta validate extensions.yaml resources.yaml \
    --crd-sources upjet-aws,upjet-azure,https://github.com/deinstapel/nats-jwt-operator

  # Fail if any CRD is missing from all sources
  crossplane beta validate extensions.yaml resources.yaml \
    --crd-sources upjet-aws,upjet-azure --fail-on-missing-crd

  # Skip composition checks (only do schema validation)
  crossplane beta validate extensions.yaml resources.yaml --skip-composition-checks

  # Validate all resources in the resourceDir folder against the extensions in the crossplane.yaml file and extensionsDir folder
  crossplane beta validate crossplane.yaml,extensionsDir/ resourceDir/

  # Validate all resources in the resources.yaml file against the extensions in the extensions.yaml file using a specific Crossplane image version
  crossplane beta validate extensions.yaml resources.yaml --crossplane-image=xpkg.crossplane.io/crossplane/crossplane:v1.20.0

  # Validate all resources in the resourceDir folder against the extensions in the extensionsDir folder and skip
  # success logs
  crossplane beta validate extensionsDir/ resourceDir/ --skip-success-results

  # Validate the output of the render command against the extensions in the extensionsDir folder
  crossplane render xr.yaml composition.yaml func.yaml --include-full-xr | crossplane beta validate extensionsDir/ -

  # Validate all resources in the resourceDir folder against the extensions in the extensionsDir folder using provided
  # cache directory and clean the cache directory before downloading schemas
  crossplane beta validate extensionsDir/ resourceDir/ --cache-dir .cache --clean-cache

  # Validate using CRD schemas from a live Kubernetes cluster (most reliable for provider schemas)
  crossplane beta validate extensionsDir/ resourceDir/ --use-cluster

  # Validate using a specific kubeconfig and context
  crossplane beta validate extensionsDir/ resourceDir/ --use-cluster --kubeconfig ~/.kube/config --kube-context my-cluster
`
}

// AfterApply implements kong.AfterApply.
func (c *Cmd) AfterApply() error {
	c.fs = afero.NewOsFs()
	return nil
}

// Run validate.
func (c *Cmd) Run(k *kong.Context, _ logging.Logger) error {
	if c.Resources == "-" && c.Extensions == "-" {
		return errors.New("cannot use stdin for both extensions and resources")
	}

	// Only set default Crossplane image if version is available
	// Version is empty in local builds (set via ldflags during release)
	if len(c.CrossplaneImage) < 1 {
		ver := version.New().GetVersionString()
		if ver != "" {
			c.CrossplaneImage = fmt.Sprintf("xpkg.crossplane.io/crossplane/crossplane:%s", ver)
		}
		// If version is empty, skip Crossplane built-in schema download
		// Users can still provide --crossplane-image explicitly
	}

	// Load all extensions
	extensionLoader, err := load.NewLoader(c.Extensions)
	if err != nil {
		return errors.Wrapf(err, "cannot load extensions from %q", c.Extensions)
	}

	extensions, err := extensionLoader.Load()
	if err != nil {
		return errors.Wrapf(err, "cannot load extensions from %q", c.Extensions)
	}

	// Load all resources
	resourceLoader, err := load.NewLoader(c.Resources)
	if err != nil {
		return errors.Wrapf(err, "cannot load resources from %q", c.Resources)
	}

	resources, err := resourceLoader.Load()
	if err != nil {
		return errors.Wrapf(err, "cannot load resources from %q", c.Resources)
	}

	if strings.HasPrefix(c.CacheDir, "~/") {
		homeDir, _ := os.UserHomeDir()
		c.CacheDir = filepath.Join(homeDir, c.CacheDir[2:])
	}

	m := NewManager(c.CacheDir, c.fs, k.Stdout, WithCrossplaneImage(c.CrossplaneImage))

	// Fetch CRDs from cluster if enabled (most reliable for provider schemas)
	var clusterFetcher *ClusterSchemaFetcher
	if c.UseCluster {
		var err error
		clusterFetcher, err = NewClusterSchemaFetcher(c.Kubeconfig, c.KubeContext, c.CacheDir, k.Stdout)
		if err != nil {
			return errors.Wrapf(err, "cannot create cluster schema fetcher")
		}

		if !c.OnlyInvalid {
			if _, err := fmt.Fprintf(k.Stdout, "Fetching CRD schemas from cluster...\n"); err != nil {
				return errors.Wrapf(err, "cannot write output")
			}
		}

		clusterCRDs, err := clusterFetcher.DiscoverAndFetch(context.Background(), extensions)
		if err != nil {
			return errors.Wrapf(err, "cannot fetch CRDs from cluster")
		}

		if !c.OnlyInvalid {
			if err := clusterFetcher.PrintDiscoveryReport(clusterCRDs, k.Stdout); err != nil {
				return errors.Wrapf(err, "cannot print cluster discovery report")
			}
		}

		// Add cluster CRDs to the manager
		m.crds = append(m.crds, clusterCRDs...)
	}

	// Fetch CRDs from external sources (GitHub repos, local paths, catalogs)
	if len(c.CRDSources) > 0 {
		sources, err := ParseCRDSources(c.CRDSources)
		if err != nil {
			return errors.Wrapf(err, "cannot parse CRD sources")
		}

		// Create fetcher with parallel option
		sourceFetcher := NewCRDSourceFetcher(c.CacheDir, k.Stdout)
		sourceFetcher.SetParallelism(c.ParallelFetch)

		// Clean cache if requested
		if c.CleanCRDCache {
			if _, err := fmt.Fprintf(k.Stdout, "Cleaning CRD source cache...\n"); err != nil {
				return errors.Wrapf(err, "cannot write output")
			}
			if err := sourceFetcher.CleanCache(); err != nil {
				return errors.Wrapf(err, "cannot clean CRD cache")
			}
		}

		var sourceCRDs []*extv1.CustomResourceDefinition
		var fetchErrs []error

		if c.PrefetchAllCRDs {
			// Prefetch ALL CRDs from each source (for Docker image caching)
			sourceCRDs, fetchErrs = sourceFetcher.PrefetchAllFromSources(context.Background(), sources)
		} else {
			// Discover all GVKs and filter out user XRDs (they come from XRD files)
			allGVKs := discoverRequiredGVKs(extensions)
			userXRDGroups := getUserXRDGroups(extensions)
			requiredGVKs := filterExternalGVKs(allGVKs, userXRDGroups)

			// Fetch only required CRDs
			sourceCRDs, err = sourceFetcher.FetchFromSources(context.Background(), sources, requiredGVKs)
			if err != nil {
				return errors.Wrapf(err, "cannot fetch CRDs from sources")
			}

			// Check for missing CRDs
			if c.FailOnMissingCRD {
				foundGVKs := make(map[string]bool)
				for _, crd := range sourceCRDs {
					for _, v := range crd.Spec.Versions {
						gvk := fmt.Sprintf("%s/%s, Kind=%s", crd.Spec.Group, v.Name, crd.Spec.Names.Kind)
						foundGVKs[gvk] = true
					}
				}
				missing := sourceFetcher.GetMissingGVKs(requiredGVKs, foundGVKs)
				if len(missing) > 0 {
					return fmt.Errorf("failed to find %d required CRDs in any source", len(missing))
				}
			}
		}

		// Report any errors from prefetch (but don't fail - we still got some CRDs)
		if len(fetchErrs) > 0 {
			if _, err := fmt.Fprintf(k.Stdout, "\n[!] %d sources had errors during fetch:\n", len(fetchErrs)); err != nil {
				return errors.Wrapf(err, "cannot write output")
			}
			for _, fetchErr := range fetchErrs {
				if _, err := fmt.Fprintf(k.Stdout, "    ⚠️  %v\n", fetchErr); err != nil {
					return errors.Wrapf(err, "cannot write output")
				}
			}
		}

		// Add source CRDs to the manager
		m.crds = append(m.crds, sourceCRDs...)
	}

	// Discover and download function input schemas
	if c.ValidateFunctionInputs {
		var dynamicClient dynamic.Interface
		if clusterFetcher != nil {
			dynamicClient = clusterFetcher.dynamicClient
		}

		fnDiscovery := NewFunctionInputDiscovery(dynamicClient, m.GetFetcher(), m.GetCache(), k.Stdout)
		fnDiscovery.DiscoverFromCompositions(extensions)

		// Resolve function packages from cluster if available
		if dynamicClient != nil {
			if err := fnDiscovery.ResolvePackagesFromCluster(context.Background()); err != nil {
				if !c.OnlyInvalid {
					if _, wErr := fmt.Fprintf(k.Stdout, "[!] Warning: cannot resolve function packages: %v\n", err); wErr != nil {
						return errors.Wrapf(wErr, "cannot write warning")
					}
				}
			}
		}

		// Download function packages and extract input schemas
		if err := fnDiscovery.DownloadInputSchemas(); err != nil {
			if !c.OnlyInvalid {
				if _, wErr := fmt.Fprintf(k.Stdout, "[!] Warning: cannot download function schemas: %v\n", err); wErr != nil {
					return errors.Wrapf(wErr, "cannot write warning")
				}
			}
		}

		// Add function input CRDs to the manager
		fnCRDs := fnDiscovery.GetInputCRDs()
		m.crds = append(m.crds, fnCRDs...)

		if !c.OnlyInvalid {
			if err := fnDiscovery.PrintDiscoveryReport(k.Stdout); err != nil {
				return errors.Wrapf(err, "cannot print function discovery report")
			}
		}
	}

	// Auto-discover providers if enabled (downloads from registry)
	if c.AutoDiscoverProviders && !c.UseCluster {
		discovery := NewProviderDiscovery()
		discovery.DiscoverFromUnstructured(extensions)

		// Only print discovery report if not in only-invalid mode
		if !c.OnlyInvalid {
			if err := discovery.PrintDiscoveryReport(k.Stdout); err != nil {
				return errors.Wrapf(err, "cannot print discovery report")
			}
		}

		// Add discovered provider objects to extensions
		providerObjs := discovery.GenerateProviderObjects()
		extensions = append(extensions, providerObjs...)

		if !c.OnlyInvalid {
			if _, err := fmt.Fprintf(k.Stdout, "Auto-discovered %d providers, downloading schemas...\n", len(providerObjs)); err != nil {
				return errors.Wrapf(err, "cannot write output")
			}
		}
	}

	// Convert XRDs/CRDs to CRDs and add package dependencies
	if err := m.PrepExtensions(extensions); err != nil {
		return errors.Wrapf(err, "cannot prepare extensions")
	}

	// Download package base layers to cache and load them as CRDs
	if err := m.CacheAndLoad(c.CleanCache); err != nil {
		return errors.Wrapf(err, "cannot download and load cache")
	}

	// Track overall validation status
	hasErrors := false

	// Determine if we should skip success logs
	skipSuccessLogs := c.SkipSuccessResults || c.OnlyInvalid

	// Pre-parse compositions to collect patch information
	// This is used to filter out false positive "Required value" errors for fields that have patches
	patchCollector := NewPatchedFieldsCollector()
	compParser := NewCompositionParser()
	allObjects := append(extensions, resources...)
	if err := compParser.Parse(allObjects); err == nil {
		for _, comp := range compParser.GetCompositions() {
			patchCollector.CollectFromComposition(comp)
		}
	}

	// 1. Schema Validation (original validation)
	// Use patch-aware validation to skip required field errors for fields that have patches
	// Resources include base resources extracted from compositions which have the annotation
	if err := SchemaValidationWithPatches(context.Background(), resources, m.crds, c.ErrorOnMissingSchemas, skipSuccessLogs, patchCollector, k.Stdout); err != nil {
		// Don't return immediately, continue with other validations
		hasErrors = true
	}

	// 1b. Also validate extensions (compositions, XRDs) against their schemas
	// This catches enum errors like "FromCompositeFieldPaths" (should be "FromCompositeFieldPath")
	// Use patch-aware validation to skip required field errors for patched fields
	if err := SchemaValidationWithPatches(context.Background(), extensions, m.crds, false, skipSuccessLogs, patchCollector, k.Stdout); err != nil {
		hasErrors = true
	}

	// 1c. Composition Structure Validation (compositeTypeRef, PatchSet references)
	if !c.SkipCompositionChecks {
		compositions, xrds := ExtractCompositionsAndXRDs(extensions)
		compValidator := NewCompositionValidator(compositions, xrds)
		compResult, err := compValidator.Validate(k.Stdout)
		if err != nil {
			return errors.Wrapf(err, "cannot validate compositions")
		}
		if len(compResult.Errors) > 0 {
			hasErrors = true
		}
		if c.StrictMode && len(compResult.Warnings) > 0 {
			hasErrors = true
		}
	}

	// 1d. Status Propagation Chain Validation
	if !c.SkipCompositionChecks && c.ValidateStatusChains {
		// Parse all compositions to get detailed patch information
		parser := NewCompositionParser()
		allObjects := append(extensions, resources...)
		if err := parser.Parse(allObjects); err != nil {
			return errors.Wrapf(err, "cannot parse compositions for status chain validation")
		}

		statusValidator := NewStatusChainValidator(parser.GetCompositions(), m.crds)
		statusIssues := statusValidator.Validate()

		// Report issues
		for _, issue := range statusIssues {
			line := ""
			if issue.SourceLine > 0 {
				line = fmt.Sprintf(":%d", issue.SourceLine)
			}
			prefix := "[x]"
			if issue.Severity == "warning" {
				prefix = "[!]"
			}
			if _, e := fmt.Fprintf(k.Stdout, "%s %s%s: %s\n", prefix, issue.SourceFile, line, issue.Message); e != nil {
				return errors.Wrap(e, errWriteOutput)
			}

			if issue.Severity == "error" {
				hasErrors = true
			} else if c.StrictMode {
				hasErrors = true
			}
		}

		// 1c. Patch Type Validation (required fields for each patch type)
		patchTypeValidator := NewPatchTypeValidator(parser.GetCompositions())
		patchTypeResult, err := patchTypeValidator.Validate(k.Stdout)
		if err != nil {
			return errors.Wrapf(err, "cannot validate patch types")
		}
		if len(patchTypeResult.Errors) > 0 {
			hasErrors = true
		}
		if c.StrictMode && len(patchTypeResult.Warnings) > 0 {
			hasErrors = true
		}

		// 1d. Composition Selector Validation
		selectorValidator := NewCompositionSelectorValidator(parser.GetCompositions())
		selectorErrors := selectorValidator.Validate()

		// Report selector issues
		for _, selectorErr := range selectorErrors {
			prefix := "[x]"
			if selectorErr.Severity == "warning" {
				prefix = "[!]"
			}
			if _, e := fmt.Fprintf(k.Stdout, "%s %s\n", prefix, selectorErr.Error()); e != nil {
				return errors.Wrap(e, errWriteOutput)
			}

			if selectorErr.Severity == "error" {
				hasErrors = true
			} else if c.StrictMode {
				hasErrors = true
			}
		}

		// 1e. Patch Type Mismatch Validation
		typeNavigator := NewSchemaNavigator(m.crds)
		typeMismatchValidator := NewPatchTypeMismatchValidator(typeNavigator, parser.GetCompositions())
		typeMismatchErrors := typeMismatchValidator.Validate()
		statusTypeErrors := typeMismatchValidator.ValidateStatusTypes()

		// Report type mismatch issues
		for _, err := range append(typeMismatchErrors, statusTypeErrors...) {
			prefix := "[x]"
			if err.Severity == "warning" {
				prefix = "[!]"
			}
			if _, e := fmt.Fprintf(k.Stdout, "%s %s\n", prefix, err.Error()); e != nil {
				return errors.Wrap(e, errWriteOutput)
			}

			if err.Severity == "error" {
				hasErrors = true
			} else if c.StrictMode {
				hasErrors = true
			}
		}
	}

	// 2. Composition Validation (patch paths, unused params)
	if !c.SkipCompositionChecks && (c.ValidatePatches || c.DetectUnusedParams) {
		config := PatchValidationConfig{
			ValidatePatchPaths: c.ValidatePatches,
			DetectUnusedParams: c.DetectUnusedParams,
			StrictMode:         c.StrictMode,
			SkipMissingSchemas: !c.ErrorOnMissingSchemas,
			ShowTree:           c.ShowTree,
			OnlyInvalid:        c.OnlyInvalid,
		}

		// Combine extensions (which contain compositions) with any compositions in resources
		allObjects := append(extensions, resources...)

		result, err := CompositionValidation(allObjects, m.crds, config, k.Stdout)
		if err != nil {
			return errors.Wrapf(err, "cannot validate compositions")
		}

		// Check for errors based on validation results
		if result.HasErrors() {
			hasErrors = true
		}

		// In strict mode, treat warnings as errors
		if c.StrictMode && result.HasWarnings() {
			hasErrors = true
		}

		// 3. Tree Analysis (if requested)
		if c.ShowTree {
			parser := NewCompositionParser()
			if err := parser.Parse(allObjects); err != nil {
				return errors.Wrapf(err, "cannot parse compositions for tree analysis")
			}

			navigator := NewSchemaNavigator(m.crds)
			treeResult, err := AnalyzePerComposition(parser, m.crds, navigator)
			if err != nil {
				return errors.Wrapf(err, "cannot analyze composition tree")
			}

			if err := PrintTreeAnalysis(treeResult, k.Stdout, c.ShowDetails); err != nil {
				return errors.Wrapf(err, "cannot print tree analysis")
			}
		}
	}

	if hasErrors {
		return errors.New("validation completed with errors")
	}

	return nil
}

// discoverRequiredGVKs discovers all GVKs that need schemas from compositions.
// Returns all GVKs found in base resources AND the extensions themselves.
func discoverRequiredGVKs(extensions []*unstructured.Unstructured) map[string]bool {
	gvks := make(map[string]bool)

	for _, obj := range extensions {
		// Add the extension object's own GVK (compositions, XRDs need validation too)
		objGVK := fmt.Sprintf("%s, Kind=%s", obj.GetAPIVersion(), obj.GetKind())
		gvks[objGVK] = true

		// Also add function input GVKs from pipeline steps
		if obj.GetAPIVersion() == "apiextensions.crossplane.io/v1" && obj.GetKind() == "Composition" {
			extractFunctionInputGVKs(obj, gvks)
		}

		// Check if this is a composition to extract base resources
		if obj.GetAPIVersion() != "apiextensions.crossplane.io/v1" || obj.GetKind() != "Composition" {
			continue
		}

		// Parse pipeline steps
		pipeline, found, _ := unstructured.NestedSlice(obj.Object, "spec", "pipeline")
		if !found {
			continue
		}

		for _, step := range pipeline {
			stepMap, ok := step.(map[string]interface{})
			if !ok {
				continue
			}

			input, ok := stepMap["input"].(map[string]interface{})
			if !ok {
				continue
			}

			resources, ok := input["resources"].([]interface{})
			if !ok {
				continue
			}

			for _, res := range resources {
				resMap, ok := res.(map[string]interface{})
				if !ok {
					continue
				}

				base, ok := resMap["base"].(map[string]interface{})
				if !ok {
					continue
				}

				apiVersion, _ := base["apiVersion"].(string)
				kind, _ := base["kind"].(string)

				if apiVersion != "" && kind != "" {
					gvk := fmt.Sprintf("%s, Kind=%s", apiVersion, kind)
					gvks[gvk] = true

					// Also check for nested resources (e.g., manifest inside Object)
					extractNestedGVKs(base, gvks)
				}
			}
		}
	}

	return gvks
}

// extractNestedGVKs extracts GVKs from nested resources (like manifest inside Object).
func extractNestedGVKs(obj map[string]interface{}, gvks map[string]bool) {
	// Check spec.forProvider.manifest
	spec, ok := obj["spec"].(map[string]interface{})
	if !ok {
		return
	}
	forProvider, ok := spec["forProvider"].(map[string]interface{})
	if !ok {
		return
	}
	manifest, ok := forProvider["manifest"].(map[string]interface{})
	if !ok {
		return
	}

	apiVersion, _ := manifest["apiVersion"].(string)
	kind, _ := manifest["kind"].(string)

	if apiVersion != "" && kind != "" {
		gvk := fmt.Sprintf("%s, Kind=%s", apiVersion, kind)
		gvks[gvk] = true
	}
}

// extractFunctionInputGVKs extracts GVKs from function pipeline inputs.
func extractFunctionInputGVKs(obj *unstructured.Unstructured, gvks map[string]bool) {
	pipeline, found, _ := unstructured.NestedSlice(obj.Object, "spec", "pipeline")
	if !found {
		return
	}

	for _, step := range pipeline {
		stepMap, ok := step.(map[string]interface{})
		if !ok {
			continue
		}

		input, ok := stepMap["input"].(map[string]interface{})
		if !ok {
			continue
		}

		// Get the function input's apiVersion and kind
		apiVersion, _ := input["apiVersion"].(string)
		kind, _ := input["kind"].(string)

		if apiVersion != "" && kind != "" {
			gvk := fmt.Sprintf("%s, Kind=%s", apiVersion, kind)
			gvks[gvk] = true
		}
	}
}

// getUserXRDGroups extracts the API groups defined by user XRDs.
// These groups have their schemas in the XRD files, not external CRDs.
func getUserXRDGroups(extensions []*unstructured.Unstructured) map[string]bool {
	groups := make(map[string]bool)
	for _, obj := range extensions {
		if obj.GetKind() == "CompositeResourceDefinition" {
			group, _, _ := unstructured.NestedString(obj.Object, "spec", "group")
			if group != "" {
				groups[group] = true
			}
		}
	}
	return groups
}

// filterExternalGVKs filters GVKs to only those that need external CRD sources.
// It excludes user-defined XRD groups (which get their schemas from the XRD files).
// Core K8s resources ARE included - they should be fetched from catalogs.
func filterExternalGVKs(allGVKs map[string]bool, userXRDGroups map[string]bool) map[string]bool {
	external := make(map[string]bool)
	for gvk := range allGVKs {
		// Parse apiVersion from GVK
		parts := strings.SplitN(gvk, ", Kind=", 2)
		if len(parts) != 2 {
			continue
		}
		apiVersion := parts[0]

		// Skip user XRD groups (validated via XRD schema from the XRD files)
		apiParts := strings.Split(apiVersion, "/")
		if len(apiParts) == 2 && userXRDGroups[apiParts[0]] {
			continue
		}

		// Include everything else (including core K8s - fetch from catalog)
		external[gvk] = true
	}
	return external
}
