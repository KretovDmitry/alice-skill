package pg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KretovDmitry/alice-skill/internal/store"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

type Store struct {
	conn *sql.DB
}

func NewStore(conn *sql.DB) *Store {
	return &Store{conn: conn}
}

func (s Store) Bootstrap(ctx context.Context) error {
	tx, err := s.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	defer tx.Rollback()

	tx.ExecContext(ctx, `
        CREATE TABLE users (
            id varchar(128) PRIMARY KEY,
            username varchar(128)
        )
    `)
	tx.ExecContext(ctx, `CREATE UNIQUE INDEX sender_idx ON users (username)`)

	tx.ExecContext(ctx, `
        CREATE TABLE messages (
            id serial PRIMARY KEY,
            sender varchar(128),
            recipient varchar(128),
            payload text,
            sent_at timestamp with time zone,
            read_at timestamp with time zone DEFAULT NULL
        )
    `)
	tx.ExecContext(ctx, `CREATE INDEX recipient_idx ON messages (recipient)`)

	return tx.Commit()
}

func (s Store) FindRecipient(ctx context.Context, username string) (userID string, err error) {
	row := s.conn.QueryRowContext(ctx, `SELECT id FROM users WHERE username = $1`, username)
	err = row.Scan(&userID)
	return
}

func (s Store) ListMessages(ctx context.Context, userID string) ([]store.Message, error) {
	// запрашиваем данные обо всех сообщениях пользователя, без самого текста
	rows, err := s.conn.QueryContext(ctx, `
        SELECT
            m.id,
            u.username AS sender,
            m.sent_at
        FROM messages m
        JOIN users u ON m.sender = u.id
        WHERE
            m.recipient = $1
    `, userID)

	if err != nil {
		return nil, err
	}
	// не забываем закрыть курсор после завершения работы с данными
	defer rows.Close()

	var messages []store.Message
	for rows.Next() {
		var m store.Message
		if err := rows.Scan(&m.ID, &m.Sender, &m.Time); err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return messages, nil
}

func (s Store) GetMessage(ctx context.Context, id int64) (*store.Message, error) {
	row := s.conn.QueryRowContext(ctx, `
        SELECT
            m.id,
            u.username AS sender,
            m.payload,
            m.sent_at
        FROM messages m
        JOIN users u ON m.sender = u.id
        WHERE
            m.id = $1
    `,
		id,
	)

	var msg store.Message
	err := row.Scan(&msg.ID, &msg.Sender, &msg.Payload, &msg.Time)
	if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (s Store) SaveMessage(ctx context.Context, userID string, msg store.Message) error {
	_, err := s.conn.ExecContext(ctx, `
        INSERT INTO messages
        (sender, recipient, payload, sent_at)
        VALUES
        ($1, $2, $3, $4);
    `, msg.Sender, userID, msg.Payload, time.Now())

	return err
}

func (s Store) SaveMessages(ctx context.Context, messages ...store.Message) error {
	// соберём данные для создания запроса с групповой вставкой
	var values []string
	var args []any
	for i, msg := range messages {
		// в нашем запросе по 4 параметра на каждое сообщение
		base := i * 4
		// PostgreSQL требует шаблоны в формате ($1, $2, $3, $4) для каждой вставки
		params := fmt.Sprintf("($%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4)
		values = append(values, params)
		args = append(args, msg.Sender, msg.Recepient, msg.Payload, msg.Time)
	}

	// составляем строку запроса
	query := `
	  INSERT INTO messages
	  (sender, recepient, payload, sent_at)
	  VALUES ` + strings.Join(values, ",") + `;`

	// добавляем новые сообщения в БД
	_, err := s.conn.ExecContext(ctx, query, args...)

	return err
}

func (s Store) RegisterUser(ctx context.Context, userID, username string) error {
	// добавляем новую запись пользователя
	_, err := s.conn.ExecContext(ctx, `
        INSERT INTO users
        (id, username)
        VALUES
        ($1, $2);
    `, userID, username)

	if err != nil {
		// проверяем, что ошибка сигнализирует о потенциальном нарушении целостности данных
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgerrcode.IsIntegrityConstraintViolation(pgErr.Code) {
			err = store.ErrConflict
		}
	}

	return err
}
