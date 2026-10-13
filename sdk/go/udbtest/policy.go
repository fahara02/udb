package udbtest

import (
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Policy is the same file schema read by `udb policy diff|apply`.
// Conditions compare request attributes by equality; the current data-plane
// gate supplies an empty attribute map. Deny ignores required_scopes, exactly
// as the broker does, so dropping a scope cannot cancel a deny.
type Policy struct {
	ID             string            `json:"id" yaml:"id"`
	Priority       int32             `json:"priority" yaml:"priority"`
	Enabled        *bool             `json:"enabled" yaml:"enabled"`
	Effect         string            `json:"effect" yaml:"effect"`
	Tenant         string            `json:"tenant" yaml:"tenant"`
	Project        string            `json:"project" yaml:"project"`
	Subject        string            `json:"subject" yaml:"subject"`
	Role           string            `json:"role" yaml:"role"`
	Action         string            `json:"action" yaml:"action"`
	Resource       string            `json:"resource" yaml:"resource"`
	Purpose        string            `json:"purpose" yaml:"purpose"`
	Relationship   string            `json:"relationship" yaml:"relationship"`
	Conditions     map[string]string `json:"conditions" yaml:"conditions"`
	RequiredScopes []string          `json:"required_scopes" yaml:"required_scopes"`
}

// Identity is a broker-verified fake identity, provisioned with an API key.
// Request headers cannot add scopes, roles or change its tenant/project/owner.
type Identity struct {
	TenantID        string
	ProjectID       string
	Subject         string
	UserID          string
	ServiceIdentity string
	Scopes          []string
	Roles           []string
}

type relationship struct{ subject, relation, object, tenant, project string }
type fakePolicySet struct {
	policies     []Policy
	defaultAllow bool
	tuples       []relationship
}

func loadPolicies(path string) ([]Policy, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("udbtest: policy file: %w", err)
	}
	defer file.Close()
	decoder := yaml.NewDecoder(file)
	var node yaml.Node
	if err := decoder.Decode(&node); err != nil {
		return nil, fmt.Errorf("udbtest: parse policy file: %w", err)
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("udbtest: policy file must contain one document")
	}
	if len(node.Content) != 1 {
		return nil, fmt.Errorf("udbtest: empty policy document")
	}
	var policies []Policy
	switch node.Content[0].Kind {
	case yaml.SequenceNode:
		err = node.Decode(&policies)
	case yaml.MappingNode:
		var document struct {
			Tenant   string    `yaml:"tenant"`
			Policies *[]Policy `yaml:"policies"`
		}
		err = node.Decode(&document)
		if err == nil && document.Policies == nil {
			return nil, fmt.Errorf("udbtest: policy document requires policies")
		}
		if err == nil {
			policies = *document.Policies
			for i := range policies {
				if strings.TrimSpace(policies[i].Tenant) == "" {
					policies[i].Tenant = document.Tenant
				}
			}
		}
	default:
		return nil, fmt.Errorf("udbtest: expected a policy list or policies document")
	}
	if err != nil {
		return nil, fmt.Errorf("udbtest: policy schema: %w", err)
	}
	for _, policy := range policies {
		if policy.Effect != "" && policy.Effect != "allow" && policy.Effect != "deny" {
			return nil, fmt.Errorf("udbtest: policy %q has invalid effect", policy.ID)
		}
	}
	return policies, nil
}

func wildcard(pattern, value string) bool {
	pattern = strings.TrimSpace(pattern)
	return pattern == "" || pattern == "*" || pattern == value
}
func policyPattern(pattern, value string) bool {
	pattern = strings.TrimSpace(pattern)
	if wildcard(pattern, value) {
		return true
	}
	if prefix, ok := strings.CutSuffix(pattern, ".*"); ok {
		return value == prefix || strings.HasPrefix(value, prefix+".")
	}
	return false
}
func hasScope(identity Identity, scope string) bool {
	if strings.TrimSpace(scope) == "" {
		return true
	}
	for _, granted := range identity.Scopes {
		if granted == scope || granted == "*" || granted == "udb:*" {
			return true
		}
	}
	return false
}
func (p *fakePolicySet) allows(identity Identity, resource, action, purpose string) bool {
	allowed := len(p.policies) == 0 && p.defaultAllow
	for _, policy := range p.policies {
		if policy.Enabled != nil && !*policy.Enabled {
			continue
		}
		if !wildcard(policy.Tenant, identity.TenantID) || !wildcard(policy.Project, identity.ProjectID) ||
			!policyPattern(policy.Action, action) || !policyPattern(policy.Resource, resource) || !wildcard(policy.Purpose, purpose) {
			continue
		}
		subject := strings.TrimSpace(policy.Subject)
		if subject != "" && subject != "*" && subject != identity.Subject && subject != identity.UserID && subject != identity.ServiceIdentity {
			continue
		}
		if role := strings.TrimSpace(policy.Role); role != "" && role != "*" {
			found := false
			for _, roleValue := range identity.Roles {
				if roleValue == role {
					found = true
				}
			}
			if !found {
				continue
			}
		}
		// The native data-plane AuthzQuery uses no caller-asserted attributes.
		if len(policy.Conditions) != 0 {
			continue
		}
		if policy.Relationship != "" {
			related := false
			for _, tuple := range p.tuples {
				if tuple.relation == policy.Relationship && tuple.object == resource &&
					(tuple.subject == identity.Subject || tuple.subject == identity.UserID || tuple.subject == identity.ServiceIdentity) &&
					wildcard(tuple.tenant, identity.TenantID) && wildcard(tuple.project, identity.ProjectID) {
					related = true
				}
			}
			if !related {
				continue
			}
		}
		if policy.Effect == "deny" {
			return false
		}
		scoped := true
		for _, scope := range policy.RequiredScopes {
			if !hasScope(identity, scope) {
				scoped = false
			}
		}
		if scoped {
			allowed = true
		}
	}
	return allowed
}

// AddRelationship provisions the same scoped subject/relation/object tuple
// the broker's relationship store consults. It does not change policy files.
func (f *Fake) AddRelationship(subject, relation, object, tenant, project string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.policy.tuples = append(f.policy.tuples, relationship{subject, relation, object, tenant, project})
}
