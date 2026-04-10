# D1 JDBC Driver

**A JDBC driver for [Cloudflare's D1 Database](https://blog.cloudflare.com/introducing-d1/) product!**

JDBC is the technology that drives popular database tools such as JetBrains' database functionality integrated into their editors and [DataGrip](https://www.jetbrains.com/datagrip/), their standalone DB editor.

These tools all require a JDBC driver — an adapter that interfaces between the GUI and the database.

The D1 JDBC Driver connects these IDEs to Cloudflare's D1 and provides:

- **Schema introspection** — automatic discovery of tables, columns, primary keys, and foreign keys
- **SQL execution** — run queries and see auto-formatted results
- **Full SQLite type affinity support** — INTEGER, REAL, TEXT, BLOB, NUMERIC
- **Batch execution** — `addBatch()` / `executeBatch()` send all statements in a single D1 API call
- **Generated key retrieval** — `getGeneratedKeys()` returns the last inserted row ID via `meta.last_row_id`
- **Accurate update counts** — `executeUpdate()` and `getUpdateCount()` reflect `meta.changes` from D1
- **Retry logic** — read-only queries (SELECT, EXPLAIN, WITH) are retried up to 3 times with exponential back-off on transient errors or HTTP 429 rate limits
- **PRAGMA compatibility** — PRAGMAs not exposed by D1 (`database_list`, `journal_mode`, `encoding`, etc.) are intercepted and return sensible static responses so introspection tools work without errors
- **Internal table filtering** — Cloudflare-internal (`_cf_*`) and SQLite system tables are hidden from schema browsers

Known issues:
- Open an issue if you find something

For more info and to get help, join us on the [Cloudflare Developers Discord](https://discord.gg/cloudflaredev)!

---

## Installation

1. Download the latest release of the D1 JDBC Driver from [here](https://github.com/afkfish/d1-jdbc-driver/releases).
2. Move the `.jar` file somewhere permanent on your computer.
3. Open DataGrip (or another JetBrains tool) and add a new **Database Driver**. (Figure 1)
4. Name it **D1 JDBC Driver**. Click the **+** beside *Driver Files*, select *Custom JARs*, then navigate to and choose the downloaded `.jar` file.
5. Select `org.isaacmcfadyen.D1Driver` from the class dropdown. (Figure 2)
6. Go to the **Options** tab, scroll to the bottom, and change the *Dialect* to **SQLite**. This is required for proper autocomplete. (Figure 3)
7. Go to the **Data Sources** panel, add a new Data Source, and select **D1 JDBC Driver**. (Figure 4)
8. Fill in the connection details: (Figure 5)
   - **URL** — `jdbc:d1://<DATABASE_UUID>` where `<DATABASE_UUID>` is your D1 database ID (shown in the Cloudflare dashboard under **Workers & Pages → D1**).
   - **User** — Your Cloudflare **Account ID**. Find it on the [Cloudflare dashboard](https://dash.cloudflare.com/) in the right-hand sidebar of the home page, or in the URL of any Workers & Pages page (the alphanumeric string after `cloudflare.com/`).
   - **Password** — A Cloudflare [API Token](https://dash.cloudflare.com/?to=/profile/api-tokens) with at minimum the **D1 Edit** permission scope.
9. Click **Test Connection**. You should see a green success message. If it fails, double-check the URL, Account ID, and API token. (Figure 6)

Figure 1:
<img width="224" alt="Figure 1" src="https://user-images.githubusercontent.com/6243993/177220616-a1c0555c-e54e-475d-b667-c0a9b327ce78.png">

Figure 2:
<img width="803" alt="Selecting Driver Class" src="https://user-images.githubusercontent.com/6243993/177220676-ba7b3598-d79e-4178-a495-cf2680c7c6e9.png">

Figure 3:
<img width="799" alt="Selecting Dialect" src="https://user-images.githubusercontent.com/6243993/177220738-1a1b277a-8d91-470c-ad6c-b61c59b786fb.png">

Figure 4:
<img width="286" alt="Selecting Data Source" src="https://user-images.githubusercontent.com/6243993/177220780-c28e6f56-b4b5-47ad-a3d1-0d113e5f1830.png">

Figure 5:
<img width="798" alt="Example of Authentication" src="https://user-images.githubusercontent.com/6243993/177220872-78a4b7a3-568b-4926-a544-63d10ee631a9.png">

Figure 6:
<img width="362" alt="Connection Succeeded" src="https://user-images.githubusercontent.com/6243993/177220909-ec0250fa-4dc2-4abf-a27a-37aff8592c5c.png">
