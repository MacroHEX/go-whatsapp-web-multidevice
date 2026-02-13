package cmd

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"os"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/store/sqlstore"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/config"
	domainApp "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/app"
	domainChat "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chat"
	domainChatStorage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/chatstorage"
	domainDevice "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/device"
	domainGroup "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/group"
	domainMessage "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/message"
	domainNewsletter "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/newsletter"
	domainSend "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/send"
	domainUser "github.com/aldinokemal/go-whatsapp-web-multidevice/domains/user"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/chatstorage"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/infrastructure/whatsapp"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/utils"
	"github.com/aldinokemal/go-whatsapp-web-multidevice/usecase"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.mau.fi/whatsmeow"
)

var (
	EmbedIndex embed.FS
	EmbedViews embed.FS

	// Whatsapp
	whatsappCli *whatsmeow.Client

	// Chat Storage
	chatStorageDB   *sql.DB
	chatStorageRepo domainChatStorage.IChatStorageRepository

	// Usecase
	appUsecase        domainApp.IAppUsecase
	chatUsecase       domainChat.IChatUsecase
	sendUsecase       domainSend.ISendUsecase
	userUsecase       domainUser.IUserUsecase
	messageUsecase    domainMessage.IMessageUsecase
	groupUsecase      domainGroup.IGroupUsecase
	newsletterUsecase domainNewsletter.INewsletterUsecase
	deviceUsecase     domainDevice.IDeviceUsecase
)

var rootCmd = &cobra.Command{
	Short: "Send free whatsapp API",
	Long:  `This application is from clone https://github.com/aldinokemal/go-whatsapp-web-multidevice`,
}

func init() {
	utils.LoadConfig(".")
	time.Local = time.UTC
	rootCmd.CompletionOptions.DisableDefaultCmd = true
	initFlags()
	cobra.OnInitialize(initEnvConfig, initApp)
}

func initEnvConfig() {
	// Application settings
	if v := viper.GetString("app_port"); v != "" {
		config.AppPort = v
	}
	if v := viper.GetString("app_host"); v != "" {
		config.AppHost = v
	}
	if v := viper.GetBool("app_debug"); v {
		config.AppDebug = v
	}
	if v := viper.GetString("app_os"); v != "" {
		config.AppOs = v
	}
	if v := viper.GetString("app_basic_auth"); v != "" {
		config.AppBasicAuthCredential = strings.Split(v, ",")
	}
	if v := viper.GetString("app_base_path"); v != "" {
		config.AppBasePath = v
	}

	// Database settings
	if v := viper.GetString("db_uri"); v != "" {
		config.DBURI = v
	}
	if v := viper.GetString("chat_storage_uri"); v != "" {
		config.ChatStorageURI = v
	}
	if v := viper.GetString("db_keys_uri"); v != "" {
		config.DBKeysURI = v
	}

	// WhatsApp settings
	if v := viper.GetString("whatsapp_auto_reply"); v != "" {
		config.WhatsappAutoReplyMessage = v
	}
	if viper.IsSet("whatsapp_auto_mark_read") {
		config.WhatsappAutoMarkRead = viper.GetBool("whatsapp_auto_mark_read")
	}
	if v := viper.GetString("whatsapp_webhook"); v != "" {
		config.WhatsappWebhook = strings.Split(v, ",")
	}
	if v := viper.GetString("whatsapp_webhook_secret"); v != "" {
		config.WhatsappWebhookSecret = v
	}
}

func initFlags() {
	rootCmd.PersistentFlags().StringVarP(&config.AppPort, "port", "p", config.AppPort, "port number")
	rootCmd.PersistentFlags().StringVarP(&config.AppHost, "host", "H", config.AppHost, "host to bind")
	rootCmd.PersistentFlags().BoolVarP(&config.AppDebug, "debug", "d", config.AppDebug, "debug mode")
	rootCmd.PersistentFlags().StringVarP(&config.DBURI, "db-uri", "", config.DBURI, "database uri")
	rootCmd.PersistentFlags().StringVarP(&config.ChatStorageURI, "chat-storage-uri", "", config.ChatStorageURI, "chat storage uri")
}

func initChatStorage() (*sql.DB, error) {
	var db *sql.DB
	var err error

	if strings.HasPrefix(config.ChatStorageURI, "postgres://") {
		db, err = sql.Open("postgres", config.ChatStorageURI)
	} else {
		return nil, fmt.Errorf("SQLite is disabled in this build. Please use a postgres:// URI")
	}

	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

func initApp() {
	if config.AppDebug {
		config.WhatsappLogLevel = "DEBUG"
		logrus.SetLevel(logrus.DebugLevel)
	}

	_ = utils.CreateFolder(config.PathQrCode, config.PathSendItems, config.PathStorages, config.PathMedia)

	ctx := context.Background()
	var err error
	chatStorageDB, err = initChatStorage()
	if err != nil {
		logrus.Fatalf("failed to initialize chat storage: %v", err)
	}

	chatStorageRepo = chatstorage.NewStorageRepository(chatStorageDB)
	_ = chatStorageRepo.InitializeSchema()

	whatsappDB := whatsapp.InitWaDB(ctx, config.DBURI)
	var keysDB *sqlstore.Container
	if config.DBKeysURI != "" {
		keysDB = whatsapp.InitWaDB(ctx, config.DBKeysURI)
	}

	whatsappCli = whatsapp.InitWaCLI(ctx, whatsappDB, keysDB, chatStorageRepo)

	dm := whatsapp.GetDeviceManager()
	if dm != nil {
		_ = dm.LoadExistingDevices(ctx)
	}

	appUsecase = usecase.NewAppService(chatStorageRepo, dm)
	chatUsecase = usecase.NewChatService(chatStorageRepo)
	sendUsecase = usecase.NewSendService(appUsecase, chatStorageRepo)
	userUsecase = usecase.NewUserService()
	messageUsecase = usecase.NewMessageService(chatStorageRepo)
	groupUsecase = usecase.NewGroupService()
	newsletterUsecase = usecase.NewNewsletterService()
	deviceUsecase = usecase.NewDeviceService(dm)
}

func Execute(embedIndex embed.FS, embedViews embed.FS) {
	EmbedIndex = embedIndex
	EmbedViews = embedViews
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
