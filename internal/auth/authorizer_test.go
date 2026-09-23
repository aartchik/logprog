package auth_test

import (
	"path/filepath"
	"testing"

	"logprog/internal/auth"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthorizer(t *testing.T) {
	model := filepath.Join("..", "..", "test", "model.conf")
	policy := filepath.Join("..", "..", "test", "policy.csv")
	authorizer := auth.New(model, policy)

	tests := map[string]struct {
		subject string
		object  string
		action  string
		code    codes.Code
	}{
		"root can produce": {
			subject: "root",
			object:  "*",
			action:  "produce",
			code:    codes.OK,
		},
		"root can consume": {
			subject: "root",
			object:  "*",
			action:  "consume",
			code:    codes.OK,
		},
		"unknown subject cannot produce": {
			subject: "nobody",
			object:  "*",
			action:  "produce",
			code:    codes.PermissionDenied,
		},
		"root cannot delete": {
			subject: "root",
			object:  "*",
			action:  "delete",
			code:    codes.PermissionDenied,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := authorizer.Authorize(test.subject, test.object, test.action)
			require.Equal(t, test.code, status.Code(err))
		})
	}
}
