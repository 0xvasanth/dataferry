# TLS

Both built-in connectors share one TLS policy:

- **Remote hosts use TLS by default**, and a connection that asks for TLS
  never falls back to plaintext.
- **Certificates are always verified**, chain and hostname. There is no
  "encrypt but don't verify" mode, and no way to turn the checks off. To
  trust a private CA, give its certificate.
- **Local connections may use plaintext.** When nothing is said, a
  connection to a remote host requires TLS. One whose hosts are all local
  (`localhost`, a loopback address such as `127.0.0.1` or `::1`, or a Unix
  socket path) may use plaintext: PostgreSQL tries TLS and falls back,
  MongoDB connects to a single local host in plaintext (details below).
  Private network addresses (`10.x`, `192.168.x`) count as remote.
- For PostgreSQL, TLS is done with rustls, and without a CA file the
  trusted roots are Mozilla's. The MongoDB driver does its own TLS.

TLS settings go in each connector's URL, in the form its ecosystem uses.

## PostgreSQL

| URL parameter | Meaning |
| --- | --- |
| `sslmode=disable` | Plaintext. |
| `sslmode=prefer`, `allow`, or no `sslmode` | TLS for remote hosts (required, verified); for local ones, TLS if the server offers it, plaintext otherwise. |
| `sslmode=require`, `verify-ca`, `verify-full` | TLS, required. All three verify the chain **and** the hostname: `require` and `verify-ca` are as strict as `verify-full` here. |
| `sslrootcert=<path>` | A PEM file of CA certificates to trust instead of the default roots. `sslrootcert=system` uses the default roots. The file is read when the first connection is made, so a wrong path fails then, not in `validate`. |

```text
postgres://app@db.example.com:5432/shop?sslmode=verify-full&sslrootcert=/etc/ssl/corp-ca.pem
host=db.example.com user=app dbname=shop sslmode=require
```

- Client certificates are not supported yet: `sslcert`, `sslkey` and
  `sslpassword` are refused ("client certificates are not supported
  yet").
- A connection over a Unix socket only is always plaintext, whatever
  `sslmode` says.
- With `hostaddr`, both `host` (the name the certificate is checked
  against) and `hostaddr` must be local for the plaintext fallback.
- An invalid `sslmode` is an error that lists the valid ones. Errors never
  echo the connection string.

## MongoDB

| URL option | Meaning |
| --- | --- |
| none | TLS for remote hosts. A single local host stays plaintext (and connects directly, `directConnection=true`). Several local hosts, or a local replica set, use TLS. |
| `tls=true`, or any TLS option such as `tlsCAFile` | TLS, required. |
| `tls=false` | Plaintext. |
| `tlsCAFile=<path>` | A PEM file of CA certificates to trust. |
| `tlsInsecure`, `tlsAllowInvalidCertificates` | Refused: "TLS certificate and hostname checks can't be turned off; to trust a private CA, set tlsCAFile". |

```text
mongodb://app@mongo-1.example.com:27017,mongo-2.example.com:27017/?replicaSet=rs0&tlsCAFile=/etc/ssl/corp-ca.pem
mongodb+srv://app@cluster0.example.mongodb.net/
```

A `mongodb+srv://` name is a remote host, so it gets TLS. The MongoDB
driver does the TLS handshake itself, with its own certificate handling;
dataferry decides whether TLS is used and refuses the options that would
disable verification. Client certificate options are passed to the driver
unchecked.

## In tests and local development

Containers on `localhost` need no TLS settings: the local fallback
applies, as in the [quickstart](../quickstart.md). For a database on
another host without TLS (a CI service container reached by name, for
example), say so explicitly: `sslmode=disable` for Postgres, `tls=false`
for MongoDB.
