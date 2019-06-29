use Mix.Config

config :pg_cdc,
  clients_db: [
    hostname: "localhost",
    database: "scholarship_manager_development"
  ],
  nats: [
    host: "localhost",
    port: 4222,
    cluster_id: "test-cluster"
  ]

config :logger,
  level: :debug
