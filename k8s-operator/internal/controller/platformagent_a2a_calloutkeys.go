/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"

	"github.com/nats-io/nkeys"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	agentv1alpha1 "github.com/gke-labs/kube-agents/k8s-operator/api/v1alpha1"
)

// The auth callout's two keypairs.
//
// **The issuer seed is the most powerful secret in this deployment, and it is
// worth being blunt about why.** It signs the user JWTs that carry the publish
// and subscribe permissions the server then enforces, so it does not merely
// authenticate connections — it decides what every connection may do. Whoever
// holds it can issue themselves a user with any grants at all, including read
// across the capability bucket the envelope design reserves. It wants
// gateway-grade custody, a rotation story and a compromise runbook, not the
// handling a thing described as "authenticating connections" would get. The
// capability envelope design (architecture 09, "one cryptographic key does
// exist") owns that analysis; this is the object it is talking about.
//
// So: the seeds live in their own Secret, mounted by the callout Deployment and
// by nothing else. They are deliberately NOT in the per-user creds Secret,
// which the gateway, the agent pod and the session spawner all read.
//
// The server holds only the public halves, in nats.conf. It never sees a seed.

const (
	// a2aCalloutIssuerSeedKey is the account seed (SA...) the callout signs
	// user JWTs with.
	a2aCalloutIssuerSeedKey = "issuer-seed"

	// a2aCalloutIssuerPubKey is the account public key (A...) that goes into
	// nats.conf as auth_callout.issuer.
	a2aCalloutIssuerPubKey = "issuer-public"

	// a2aCalloutXKeySeedKey is the curve seed (SX...) the callout decrypts
	// authorization requests with.
	a2aCalloutXKeySeedKey = "xkey-seed"

	// a2aCalloutXKeyPubKey is the curve public key (X...) that goes into
	// nats.conf as auth_callout.xkey.
	a2aCalloutXKeyPubKey = "xkey-public"
)

// a2aCalloutKeysName is the Secret holding the callout's keypairs.
func a2aCalloutKeysName(agent *agentv1alpha1.PlatformAgent) string {
	return a2aCalloutName(agent) + "-keys"
}

// a2aCalloutKeys is the rendered view: seeds for the callout, public halves for
// the server config.
type a2aCalloutKeys struct {
	IssuerSeed   string
	IssuerPublic string
	XKeySeed     string
	XKeyPublic   string
}

// ensureA2ACalloutKeysSecret creates the keypairs once and then leaves them
// alone.
//
// Create-once matters more here than for the passwords. Rotating the issuer is
// not a credential refresh: the public half is in nats.conf, so a new key is a
// new server config, and the server refuses a config reload that touches the
// auth_callout block at all — the reload fails outright and takes every other
// pending change in the file with it. So a rotation is a NATS restart, and a
// restart while the callout is serving a key the server no longer trusts
// refuses every new connection in between. Regenerating these on reconcile
// would do that every few seconds.
//
// A partially-populated Secret is repaired as a set rather than per key: the
// four values are two keypairs, and filling in a seed without its public half
// would leave the server trusting a key nothing holds.
func (r *PlatformAgentReconciler) ensureA2ACalloutKeysSecret(ctx context.Context, agent *agentv1alpha1.PlatformAgent) (*a2aCalloutKeys, error) {
	name := types.NamespacedName{Name: a2aCalloutKeysName(agent), Namespace: agent.Namespace}

	existing := &corev1.Secret{}
	err := r.a2aReader().Get(ctx, name, existing)
	if err == nil {
		keys, ok := readA2ACalloutKeys(existing)
		if ok {
			return keys, nil
		}
		// Incomplete or corrupt. Regenerate the whole set — see above on
		// why this is not a thing to do by halves.
		generated, data, gerr := generateA2ACalloutKeys()
		if gerr != nil {
			return nil, gerr
		}
		existing.Data = data
		if err := r.Update(ctx, existing); err != nil {
			return nil, err
		}
		return generated, nil
	}
	if !errors.IsNotFound(err) {
		return nil, err
	}

	generated, data, err := generateA2ACalloutKeys()
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace, Labels: a2aLabels(agent, "callout-keys")},
		Data:       data,
	}
	if err := ctrl.SetControllerReference(agent, secret, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, secret); err != nil {
		return nil, err
	}
	return generated, nil
}

func readA2ACalloutKeys(secret *corev1.Secret) (*a2aCalloutKeys, bool) {
	keys := &a2aCalloutKeys{
		IssuerSeed:   string(secret.Data[a2aCalloutIssuerSeedKey]),
		IssuerPublic: string(secret.Data[a2aCalloutIssuerPubKey]),
		XKeySeed:     string(secret.Data[a2aCalloutXKeySeedKey]),
		XKeyPublic:   string(secret.Data[a2aCalloutXKeyPubKey]),
	}
	if keys.IssuerSeed == "" || keys.IssuerPublic == "" || keys.XKeySeed == "" || keys.XKeyPublic == "" {
		return nil, false
	}
	// The key TYPE is checked, not just presence. The server validates
	// auth_callout.issuer as a public ACCOUNT nkey and auth_callout.xkey as a
	// public curve key, and it refuses to start on either being wrong — so a
	// Secret hand-edited with a user key would take the bus down at the next
	// config change rather than at the edit.
	if prefix, _, err := nkeys.DecodeSeed([]byte(keys.IssuerSeed)); err != nil || prefix != nkeys.PrefixByteAccount {
		return nil, false
	}
	if prefix, _, err := nkeys.DecodeSeed([]byte(keys.XKeySeed)); err != nil || prefix != nkeys.PrefixByteCurve {
		return nil, false
	}
	return keys, true
}

func generateA2ACalloutKeys() (*a2aCalloutKeys, map[string][]byte, error) {
	issuer, err := nkeys.CreateAccount()
	if err != nil {
		return nil, nil, fmt.Errorf("generating the callout issuer key: %w", err)
	}
	issuerSeed, err := issuer.Seed()
	if err != nil {
		return nil, nil, fmt.Errorf("reading the callout issuer seed: %w", err)
	}
	issuerPub, err := issuer.PublicKey()
	if err != nil {
		return nil, nil, fmt.Errorf("reading the callout issuer public key: %w", err)
	}

	// The curve key encrypts the authorization request, which carries the
	// client's raw ServiceAccount token. Without it that token crosses the
	// bus in cleartext on every single connection attempt.
	xkey, err := nkeys.CreateCurveKeys()
	if err != nil {
		return nil, nil, fmt.Errorf("generating the callout xkey: %w", err)
	}
	xkeySeed, err := xkey.Seed()
	if err != nil {
		return nil, nil, fmt.Errorf("reading the callout xkey seed: %w", err)
	}
	xkeyPub, err := xkey.PublicKey()
	if err != nil {
		return nil, nil, fmt.Errorf("reading the callout xkey public key: %w", err)
	}

	keys := &a2aCalloutKeys{
		IssuerSeed:   string(issuerSeed),
		IssuerPublic: issuerPub,
		XKeySeed:     string(xkeySeed),
		XKeyPublic:   xkeyPub,
	}
	return keys, map[string][]byte{
		a2aCalloutIssuerSeedKey: issuerSeed,
		a2aCalloutIssuerPubKey:  []byte(issuerPub),
		a2aCalloutXKeySeedKey:   xkeySeed,
		a2aCalloutXKeyPubKey:    []byte(xkeyPub),
	}, nil
}
