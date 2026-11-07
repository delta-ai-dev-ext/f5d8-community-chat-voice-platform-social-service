package models

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrorCode is the stable, machine-readable snake_case error code returned in
// the `error` field of every error response.
type ErrorCode string

// Error codes from docs/openapi_spec.yaml (plus the boundary codes used for
// authentication, malformed requests, and internal failures).
const (
	CodeSelfRequest         ErrorCode = "self_request"
	CodeAlreadyFriends      ErrorCode = "already_friends"
	CodeRequestPending      ErrorCode = "request_pending"
	CodeRequestNotPending   ErrorCode = "request_not_pending"
	CodeNotFriends          ErrorCode = "not_friends"
	CodeGroupTooLarge       ErrorCode = "group_too_large"
	CodeGroupFull           ErrorCode = "group_full"
	CodeMissingParticipants ErrorCode = "missing_participants"
	CodeNotAParticipant     ErrorCode = "not_a_participant"
	CodeGroupOnlyOperation  ErrorCode = "group_only_operation"
	CodeInvalidName         ErrorCode = "invalid_name"
	CodeIdempotencyKeyReuse ErrorCode = "idempotency_key_reuse"

	CodeUnauthorized        ErrorCode = "unauthorized"
	CodeInvalidRequest      ErrorCode = "invalid_request"
	CodeNotFound            ErrorCode = "not_found"
	CodeDatabaseUnavailable ErrorCode = "database_unavailable"
	CodeInternal            ErrorCode = "internal_error"
)

// DomainError is a rule violation carrying its wire code and HTTP status. It
// never carries internal detail: Detail is safe to return to the caller.
type DomainError struct {
	Code   ErrorCode
	Status int
	Detail string
}

// Error implements error.
func (e *DomainError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Detail)
}

// NewDomainError builds a domain error with an explicit status.
func NewDomainError(code ErrorCode, status int, detail string) *DomainError {
	return &DomainError{Code: code, Status: status, Detail: detail}
}

// ValidationError is a 422 semantic validation failure.
func ValidationError(code ErrorCode, detail string) *DomainError {
	return NewDomainError(code, http.StatusUnprocessableEntity, detail)
}

// ConflictError is a 409 conflict with existing state.
func ConflictError(code ErrorCode, detail string) *DomainError {
	return NewDomainError(code, http.StatusConflict, detail)
}

// ForbiddenError is a 403 authorization failure on an existing resource.
func ForbiddenError(code ErrorCode, detail string) *DomainError {
	return NewDomainError(code, http.StatusForbidden, detail)
}

// NotFoundResourceError is a 404 for a resource that does not exist or is not
// visible to the caller.
func NotFoundResourceError(code ErrorCode, detail string) *DomainError {
	return NewDomainError(code, http.StatusNotFound, detail)
}

// UnauthorizedError is the 401 returned when a token is missing or invalid.
func UnauthorizedError(detail string) *DomainError {
	return NewDomainError(CodeUnauthorized, http.StatusUnauthorized, detail)
}

// Documented rule violations. Constructors are used instead of package-level
// values so callers can wrap them with extra context without aliasing.
func ErrSelfRequest() *DomainError {
	return ValidationError(CodeSelfRequest, "you cannot target yourself")
}

// ErrAlreadyFriends is returned when a friendship already exists for the pair.
func ErrAlreadyFriends() *DomainError {
	return ConflictError(CodeAlreadyFriends, "a friendship already exists for this pair")
}

// ErrRequestPending is returned when a pending request exists in either
// direction for the pair.
func ErrRequestPending() *DomainError {
	return ConflictError(CodeRequestPending, "a pending friend request already exists for this pair")
}

// ErrRequestNotPending is returned when a request is no longer pending.
func ErrRequestNotPending() *DomainError {
	return ConflictError(CodeRequestNotPending, "the friend request is not pending")
}

// ErrNotFriendsValidation is the 422 form of not_friends used by conversation
// creation and participant addition.
func ErrNotFriendsValidation() *DomainError {
	return ValidationError(CodeNotFriends, "the users are not friends")
}

// ErrNotFriendsNotFound is the 404 form of not_friends used by unfriending.
func ErrNotFriendsNotFound() *DomainError {
	return NotFoundResourceError(CodeNotFriends, "the users are not friends")
}

// ErrGroupTooLarge is returned when a group creation exceeds 9 friend ids.
func ErrGroupTooLarge() *DomainError {
	return ValidationError(CodeGroupTooLarge, "a group may add at most 9 friends")
}

// ErrGroupFull is returned when an addition would exceed 10 participants.
func ErrGroupFull() *DomainError {
	return ValidationError(CodeGroupFull, "the conversation already has the maximum of 10 participants")
}

// ErrMissingParticipants is returned when group creation has no friends.
func ErrMissingParticipants() *DomainError {
	return ValidationError(CodeMissingParticipants, "at least one participant is required")
}

// ErrNotAParticipant is returned when the caller is not a current participant.
func ErrNotAParticipant() *DomainError {
	return ForbiddenError(CodeNotAParticipant, "you are not a current participant of this conversation")
}

// ErrGroupOnlyOperation is returned for group-only operations on a direct
// conversation.
func ErrGroupOnlyOperation() *DomainError {
	return ValidationError(CodeGroupOnlyOperation, "this operation is only available for group conversations")
}

// ErrInvalidName is returned for a conversation name outside 1..100 characters.
func ErrInvalidName() *DomainError {
	return ValidationError(CodeInvalidName, "name must be between 1 and 100 characters")
}

// ErrIdempotencyKeyReuse is returned when the same key is reused with a
// different request body.
func ErrIdempotencyKeyReuse() *DomainError {
	return ConflictError(CodeIdempotencyKeyReuse, "this Idempotency-Key was already used for a different request")
}

// ErrConversationNotFound is returned when a conversation does not exist.
func ErrConversationNotFound() *DomainError {
	return NotFoundResourceError(CodeNotFound, "conversation not found")
}

// ErrFriendRequestNotFound is returned when a friend request does not exist or
// the caller is not a party to it.
func ErrFriendRequestNotFound() *DomainError {
	return NotFoundResourceError(CodeNotFound, "friend request not found")
}

// AsDomainError extracts a *DomainError from an error chain.
func AsDomainError(err error) (*DomainError, bool) {
	var domainErr *DomainError
	if errors.As(err, &domainErr) {
		return domainErr, true
	}
	return nil, false
}
