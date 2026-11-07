package models

import (
	"time"

	"github.com/google/uuid"
)

// ErrorResponse is the body of every error response.
type ErrorResponse struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

// FriendRequestDTO is the FriendRequest schema of the OpenAPI spec.
type FriendRequestDTO struct {
	RequestID       string    `json:"request_id"`
	SenderUserID    string    `json:"sender_user_id"`
	RecipientUserID string    `json:"recipient_user_id"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
}

// FriendRequestPage is the FriendRequestPage schema.
type FriendRequestPage struct {
	Data       []FriendRequestDTO `json:"data"`
	NextCursor *string            `json:"next_cursor"`
}

// FriendshipDTO is the Friendship schema.
type FriendshipDTO struct {
	FriendUserID string    `json:"friend_user_id"`
	Since        time.Time `json:"since"`
}

// FriendListPage is the FriendList schema.
type FriendListPage struct {
	Data       []FriendshipDTO `json:"data"`
	NextCursor *string         `json:"next_cursor"`
}

// ConversationParticipantDTO is the ConversationParticipant schema.
type ConversationParticipantDTO struct {
	UserID   string    `json:"user_id"`
	JoinedAt time.Time `json:"joined_at"`
}

// ConversationDTO is the Conversation schema.
type ConversationDTO struct {
	ConversationID      string                       `json:"conversation_id"`
	Kind                string                       `json:"kind"`
	Name                *string                      `json:"name"`
	CreatedBy           string                       `json:"created_by"`
	Participants        []ConversationParticipantDTO `json:"participants"`
	CreatedAt           time.Time                    `json:"created_at"`
	ConversationVersion int                          `json:"conversation_version"`
}

// ConversationPage is the ConversationPage schema.
type ConversationPage struct {
	Data       []ConversationDTO `json:"data"`
	NextCursor *string           `json:"next_cursor"`
}

// NewFriendRequestDTO maps a domain friend request onto its wire shape.
func NewFriendRequestDTO(request FriendRequest) FriendRequestDTO {
	return FriendRequestDTO{
		RequestID:       request.ID.String(),
		SenderUserID:    request.SenderUserID.String(),
		RecipientUserID: request.RecipientUserID.String(),
		Status:          string(request.Status),
		CreatedAt:       request.CreatedAt.UTC(),
	}
}

// NewFriendRequestDTOs maps a slice of friend requests, always returning a
// non-nil slice so the JSON body carries `[]` rather than `null`.
func NewFriendRequestDTOs(requests []FriendRequest) []FriendRequestDTO {
	out := make([]FriendRequestDTO, 0, len(requests))
	for _, request := range requests {
		out = append(out, NewFriendRequestDTO(request))
	}
	return out
}

// NewFriendshipDTO maps a friendship row from the viewer's perspective.
func NewFriendshipDTO(friendship Friendship, viewer uuid.UUID) (FriendshipDTO, bool) {
	other, ok := friendship.Other(viewer)
	if !ok {
		return FriendshipDTO{}, false
	}
	return FriendshipDTO{FriendUserID: other.String(), Since: friendship.CreatedAt.UTC()}, true
}

// NewConversationDTO maps a conversation and its current participants.
func NewConversationDTO(conversation Conversation) ConversationDTO {
	active := conversation.ActiveParticipants()
	participants := make([]ConversationParticipantDTO, 0, len(active))
	for _, participant := range active {
		participants = append(participants, ConversationParticipantDTO{
			UserID:   participant.UserID.String(),
			JoinedAt: participant.JoinedAt.UTC(),
		})
	}
	return ConversationDTO{
		ConversationID:      conversation.ID.String(),
		Kind:                string(conversation.Kind),
		Name:                conversation.Name,
		CreatedBy:           conversation.CreatedBy.String(),
		Participants:        participants,
		CreatedAt:           conversation.CreatedAt.UTC(),
		ConversationVersion: conversation.Version,
	}
}

// NewConversationDTOs maps a slice of conversations, always returning a
// non-nil slice.
func NewConversationDTOs(conversations []Conversation) []ConversationDTO {
	out := make([]ConversationDTO, 0, len(conversations))
	for _, conversation := range conversations {
		out = append(out, NewConversationDTO(conversation))
	}
	return out
}
