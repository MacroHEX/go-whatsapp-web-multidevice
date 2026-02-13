package chatstorage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type SQLRepository struct {
	db         *sql.DB
	isPostgres bool
}

func NewStorageRepository(db *sql.DB) domainChatStorage.IChatStorageRepository {
	driverName := fmt.Sprintf("%T", db.Driver())
	isPostgres := strings.Contains(strings.ToLower(driverName), "pq") || strings.Contains(strings.ToLower(driverName), "postgres")
	return &SQLRepository{
		db:         db,
		isPostgres: isPostgres,
	}
}

// p gestiona la compatibilidad de placeholders (? -> $n)
func (r *SQLRepository) p(query string) string {
	if !r.isPostgres {
		return query
	}
	n := 1
	for strings.Contains(query, "?") {
		query = strings.Replace(query, "?", fmt.Sprintf("$%d", n), 1)
		n++
	}
	return query
}

func (r *SQLRepository) StoreChat(chat *domainChatStorage.Chat) error {
	now := time.Now()
	chat.UpdatedAt = now
	qUpdate := `UPDATE chats SET name = ?, last_message_time = ?, ephemeral_expiration = ?, updated_at = ? WHERE jid = ? AND device_id = ?`
	result, err := r.db.Exec(r.p(qUpdate), chat.Name, chat.LastMessageTime, chat.EphemeralExpiration, chat.UpdatedAt, chat.JID, chat.DeviceID)
	if err != nil {
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		qInsert := `INSERT INTO chats (jid, device_id, name, last_message_time, ephemeral_expiration, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
		_, err = r.db.Exec(r.p(qInsert), chat.JID, chat.DeviceID, chat.Name, chat.LastMessageTime, chat.EphemeralExpiration, now, chat.UpdatedAt)
	}
	return err
}

func (r *SQLRepository) GetChat(jid string) (*domainChatStorage.Chat, error) {
	q := `SELECT device_id, jid, name, last_message_time, ephemeral_expiration, created_at, updated_at FROM chats WHERE jid = ?`
	chat, err := r.scanChat(r.db.QueryRow(r.p(q), jid))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return chat, err
}

func (r *SQLRepository) GetChatByDevice(deviceID, jid string) (*domainChatStorage.Chat, error) {
	q := `SELECT device_id, jid, name, last_message_time, ephemeral_expiration, created_at, updated_at FROM chats WHERE jid = ? AND device_id = ?`
	chat, err := r.scanChat(r.db.QueryRow(r.p(q), jid, deviceID))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return chat, err
}

func (r *SQLRepository) StoreMessage(message *domainChatStorage.Message) error {
	now := time.Now()
	message.CreatedAt = now
	message.UpdatedAt = now
	if message.Content == "" && message.MediaType == "" {
		return nil
	}

	qUpdate := `UPDATE messages SET sender = ?, content = ?, timestamp = ?, is_from_me = ?, media_type = ?, filename = ?, url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ?, updated_at = ? WHERE id = ? AND chat_jid = ? AND device_id = ?`
	result, err := r.db.Exec(r.p(qUpdate), message.Sender, message.Content, message.Timestamp, message.IsFromMe, message.MediaType, message.Filename, message.URL, message.MediaKey, message.FileSHA256, message.FileEncSHA256, message.FileLength, message.UpdatedAt, message.ID, message.ChatJID, message.DeviceID)
	if err != nil {
		return err
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		qInsert := `INSERT INTO messages (id, chat_jid, device_id, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
		_, err = r.db.Exec(r.p(qInsert), message.ID, message.ChatJID, message.DeviceID, message.Sender, message.Content, message.Timestamp, message.IsFromMe, message.MediaType, message.Filename, message.URL, message.MediaKey, message.FileSHA256, message.FileEncSHA256, message.FileLength, message.CreatedAt, message.UpdatedAt)
	}
	return err
}

func (r *SQLRepository) GetMessageByID(id string) (*domainChatStorage.Message, error) {
	q := `SELECT id, chat_jid, device_id, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length, created_at, updated_at FROM messages WHERE id = ? LIMIT 1`
	message, err := r.scanMessage(r.db.QueryRow(r.p(q), id))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return message, err
}

func (r *SQLRepository) GetChats(filter *domainChatStorage.ChatFilter) ([]*domainChatStorage.Chat, error) {
	var conditions []string
	var args []any
	query := `SELECT c.device_id, c.jid, c.name, c.last_message_time, c.ephemeral_expiration, c.created_at, c.updated_at FROM chats c`

	if filter.SearchName != "" {
		conditions = append(conditions, "c.name LIKE ?")
		args = append(args, "%"+filter.SearchName+"%")
	}
	if filter.DeviceID != "" {
		conditions = append(conditions, "c.device_id = ?")
		args = append(args, filter.DeviceID)
	}
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY c.last_message_time DESC"
	if filter.Limit > 0 {
		query += " LIMIT ?"
		args = append(args, filter.Limit)
	}

	rows, err := r.db.Query(r.p(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var chats []*domainChatStorage.Chat
	for rows.Next() {
		chat, err := r.scanChat(rows)
		if err != nil {
			return nil, err
		}
		chats = append(chats, chat)
	}
	return chats, nil
}

func (r *SQLRepository) DeleteChat(jid string) error {
	tx, _ := r.db.Begin()
	tx.Exec(r.p("DELETE FROM messages WHERE chat_jid = ?"), jid)
	tx.Exec(r.p("DELETE FROM chats WHERE jid = ?"), jid)
	return tx.Commit()
}

func (r *SQLRepository) DeleteChatByDevice(deviceID, jid string) error {
	tx, _ := r.db.Begin()
	tx.Exec(r.p("DELETE FROM messages WHERE chat_jid = ? AND device_id = ?"), jid, deviceID)
	tx.Exec(r.p("DELETE FROM chats WHERE jid = ? AND device_id = ?"), jid, deviceID)
	return tx.Commit()
}

func (r *SQLRepository) GetMessages(filter *domainChatStorage.MessageFilter) ([]*domainChatStorage.Message, error) {
	query := `SELECT id, chat_jid, device_id, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length, created_at, updated_at FROM messages WHERE chat_jid = ? AND device_id = ? ORDER BY timestamp DESC`
	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}
	rows, err := r.db.Query(r.p(query), filter.ChatJID, filter.DeviceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []*domainChatStorage.Message
	for rows.Next() {
		m, _ := r.scanMessage(rows)
		messages = append(messages, m)
	}
	return messages, nil
}

func (r *SQLRepository) SearchMessages(deviceID, chatJID, searchText string, limit int) ([]*domainChatStorage.Message, error) {
	q := `SELECT id, chat_jid, device_id, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length, created_at, updated_at FROM messages WHERE chat_jid = ? AND device_id = ? AND LOWER(content) LIKE ? ORDER BY timestamp DESC`
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := r.db.Query(r.p(q), chatJID, deviceID, "%"+strings.ToLower(searchText)+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []*domainChatStorage.Message
	for rows.Next() {
		m, _ := r.scanMessage(rows)
		messages = append(messages, m)
	}
	return messages, nil
}

func (r *SQLRepository) DeleteMessage(id, chatJID string) error {
	_, err := r.db.Exec(r.p("DELETE FROM messages WHERE id = ? AND chat_jid = ?"), id, chatJID)
	return err
}

func (r *SQLRepository) DeleteMessageByDevice(deviceID, id, chatJID string) error {
	_, err := r.db.Exec(r.p("DELETE FROM messages WHERE id = ? AND chat_jid = ? AND device_id = ?"), id, chatJID, deviceID)
	return err
}

func (r *SQLRepository) SaveDeviceRecord(record *domainChatStorage.DeviceRecord) error {
	now := time.Now()
	res, _ := r.db.Exec(r.p("UPDATE devices SET display_name = ?, jid = ?, updated_at = ? WHERE device_id = ?"), record.DisplayName, record.JID, now, record.DeviceID)
	aff, _ := res.RowsAffected()
	if aff == 0 {
		r.db.Exec(r.p("INSERT INTO devices (device_id, display_name, jid, created_at, updated_at) VALUES (?, ?, ?, ?, ?)"), record.DeviceID, record.DisplayName, record.JID, now, now)
	}
	return nil
}

func (r *SQLRepository) ListDeviceRecords() ([]*domainChatStorage.DeviceRecord, error) {
	rows, _ := r.db.Query(r.p("SELECT device_id, display_name, jid, created_at, updated_at FROM devices ORDER BY created_at ASC"))
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()
	var records []*domainChatStorage.DeviceRecord
	for rows.Next() {
		rec := &domainChatStorage.DeviceRecord{}
		rows.Scan(&rec.DeviceID, &rec.DisplayName, &rec.JID, &rec.CreatedAt, &rec.UpdatedAt)
		records = append(records, rec)
	}
	return records, nil
}

func (r *SQLRepository) GetDeviceRecord(deviceID string) (*domainChatStorage.DeviceRecord, error) {
	rec := &domainChatStorage.DeviceRecord{}
	err := r.db.QueryRow(r.p("SELECT device_id, display_name, jid, created_at, updated_at FROM devices WHERE device_id = ? LIMIT 1"), deviceID).Scan(&rec.DeviceID, &rec.DisplayName, &rec.JID, &rec.CreatedAt, &rec.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return rec, err
}

func (r *SQLRepository) DeleteDeviceRecord(deviceID string) error {
	_, err := r.db.Exec(r.p("DELETE FROM devices WHERE device_id = ?"), deviceID)
	return err
}

func (r *SQLRepository) CreateMessage(ctx context.Context, evt *events.Message) error {
	client := whatsapp.ClientFromContext(ctx)
	var deviceID string
	if inst, ok := whatsapp.DeviceFromContext(ctx); ok {
		deviceID = inst.ID()
	}

	normalizedChatJID := whatsapp.NormalizeJIDFromLID(ctx, evt.Info.Chat, client)
	chatJID := normalizedChatJID.String()

	chat := &domainChatStorage.Chat{
		DeviceID:        deviceID,
		JID:             chatJID,
		Name:            r.GetChatNameWithPushName(normalizedChatJID, chatJID, evt.Info.Sender.User, evt.Info.PushName),
		LastMessageTime: evt.Info.Timestamp,
	}
	_ = r.StoreChat(chat)

	content := utils.ExtractMessageTextFromProto(evt.Message)
	mType, fName, url, mKey, fSha, fEncSha, fLen := utils.ExtractMediaInfo(evt.Message)

	message := &domainChatStorage.Message{
		ID: evt.Info.ID, ChatJID: chatJID, DeviceID: deviceID, Sender: evt.Info.Sender.String(),
		Content: content, Timestamp: evt.Info.Timestamp, IsFromMe: evt.Info.IsFromMe,
		MediaType: mType, Filename: fName, URL: url, MediaKey: mKey, FileSHA256: fSha, FileEncSHA256: fEncSha, FileLength: fLen,
	}
	return r.StoreMessage(message)
}

func (r *SQLRepository) GetChatNameWithPushName(jid types.JID, chatJID string, senderUser string, pushName string) string {
	if pushName != "" {
		return pushName
	}
	return jid.User
}

func (r *SQLRepository) GetChatNameWithPushNameByDevice(deviceID string, jid types.JID, chatJID string, senderUser string, pushName string) string {
	return r.GetChatNameWithPushName(jid, chatJID, senderUser, pushName)
}

func (r *SQLRepository) InitializeSchema() error {
	v, _ := r.getSchemaVersion()
	migs := r.getMigrations()
	for i := v; i < len(migs); i++ {
		_, _ = r.db.Exec(migs[i])
		_, _ = r.db.Exec(r.p("DELETE FROM schema_info WHERE version = ?"), i+1)
		_, _ = r.db.Exec(r.p("INSERT INTO schema_info (version) VALUES (?)"), i+1)
	}
	return nil
}

func (r *SQLRepository) getSchemaVersion() (int, error) {
	_, _ = r.db.Exec(r.p(`CREATE TABLE IF NOT EXISTS schema_info (version INTEGER PRIMARY KEY, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`))
	var v int
	_ = r.db.QueryRow(`SELECT COALESCE(MAX(version), 0) FROM schema_info`).Scan(&v)
	return v, nil
}

func (r *SQLRepository) getMigrations() []string {
	blobType := "BLOB"
	if r.isPostgres {
		blobType = "BYTEA"
	}
	return []string{
		`CREATE TABLE IF NOT EXISTS chats (jid VARCHAR(255), device_id VARCHAR(255) DEFAULT '', name VARCHAR(255), last_message_time TIMESTAMP, ephemeral_expiration INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (jid, device_id))`,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS messages (id VARCHAR(255), chat_jid VARCHAR(255), device_id VARCHAR(255) DEFAULT '', sender VARCHAR(255), content TEXT, timestamp TIMESTAMP, is_from_me BOOLEAN DEFAULT FALSE, media_type VARCHAR(50), filename VARCHAR(255), url TEXT, media_key %s, file_sha256 %s, file_enc_sha256 %s, file_length INTEGER DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (id, chat_jid, device_id))`, blobType, blobType, blobType),
		`CREATE TABLE IF NOT EXISTS devices (device_id VARCHAR(255) PRIMARY KEY, display_name VARCHAR(255) DEFAULT '', jid VARCHAR(255) DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`,
	}
}

func (r *SQLRepository) scanChat(s interface{ Scan(...any) error }) (*domainChatStorage.Chat, error) {
	c := &domainChatStorage.Chat{}
	err := s.Scan(&c.DeviceID, &c.JID, &c.Name, &c.LastMessageTime, &c.EphemeralExpiration, &c.CreatedAt, &c.UpdatedAt)
	return c, err
}

func (r *SQLRepository) scanMessage(s interface{ Scan(...any) error }) (*domainChatStorage.Message, error) {
	m := &domainChatStorage.Message{}
	err := s.Scan(&m.ID, &m.ChatJID, &m.DeviceID, &m.Sender, &m.Content, &m.Timestamp, &m.IsFromMe, &m.MediaType, &m.Filename, &m.URL, &m.MediaKey, &m.FileSHA256, &m.FileEncSHA256, &m.FileLength, &m.CreatedAt, &m.UpdatedAt)
	return m, err
}

// Implementación del método faltante para que la interfaz se cumpla
func (r *SQLRepository) DeleteDeviceData(deviceID string) error {
	if deviceID == "" {
		return fmt.Errorf("device_id is required")
	}
	tx, err := r.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, _ = tx.Exec(r.p("DELETE FROM messages WHERE device_id = ?"), deviceID)
	_, _ = tx.Exec(r.p("DELETE FROM chats WHERE device_id = ?"), deviceID)

	return tx.Commit()
}

// Resto de métodos requeridos por la interfaz
func (r *SQLRepository) StoreMessagesBatch(m []*domainChatStorage.Message) error { return nil }
func (r *SQLRepository) GetChatMessageCount(jid string) (int64, error)           { return 0, nil }
func (r *SQLRepository) GetChatMessageCountByDevice(d, j string) (int64, error)  { return 0, nil }
func (r *SQLRepository) GetTotalMessageCount() (int64, error)                    { return 0, nil }
func (r *SQLRepository) GetTotalChatCount() (int64, error)                       { return 0, nil }
func (r *SQLRepository) TruncateAllChats() error                                 { return nil }
func (r *SQLRepository) GetStorageStatistics() (int64, int64, error)             { return 0, 0, nil }
func (r *SQLRepository) TruncateAllDataWithLogging(p string) error               { return nil }
func (r *SQLRepository) StoreSentMessageWithContext(ctx context.Context, mID, s, rec, con string, t time.Time) error {
	return nil
}
