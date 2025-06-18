open Lwt.Infix

let escape_single_quote s =
  let buf = Buffer.create (String.length s) in
  String.iter
    (function '\'' -> Buffer.add_string buf "''" | c -> Buffer.add_char buf c)
    s;
  Buffer.contents buf

module Store = struct
  let string_of_date (x : Model.date) =
    Printf.sprintf "%04d-%02d-%02d" x.year x.month x.day

  let string_of_account account = String.concat ":" account

  module Q = struct
    let create_accounts =
      {|
CREATE TABLE accounts (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  currency TEXT NOT NULL,
  parent_id INTEGER,
  kind INTEGER NOT NULL,

  UNIQUE (name, currency, parent_id)
) STRICT|}

    let create_transactions =
      {|
CREATE TABLE transactions (
  id INTEGER PRIMARY KEY,
  created_at TEXT NOT NULL,
  narration TEXT NOT NULL
) STRICT|}

    let create_postings =
      {|
CREATE TABLE postings (
  id INTEGER PRIMARY KEY,
  account_id INTEGER NOT NULL,
  transaction_id INTEGER NOT NULL,
  amount INTEGER NOT NULL,
  narration TEXT NOT NULL,
  cost INTEGER,
  price INTEGER,
  FOREIGN KEY (account_id) REFERENCES accounts (id),
  FOREIGN KEY (transaction_id) REFERENCES transactions (id)
) STRICT|}

    let create_full_accounts_view =
      {|
CREATE VIEW full_accounts AS
WITH RECURSIVE tree(id, name, currency, parent_id, kind, depth) AS (
  SELECT id, name, currency, parent_id, kind, 0 FROM accounts WHERE parent_id IS NULL
  UNION ALL
  SELECT a.id, t.name || ':' || a.name, a.currency, a.parent_id, a.kind, t.depth + 1 FROM accounts a JOIN tree t ON a.parent_id = t.id
)
SELECT id, name, currency, kind, depth FROM tree
|}

    let create_tags =
      {|
CREATE TABLE tags (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
) STRICT|}

    let create_transaction_tag =
      {|
CREATE TABLE transaction_tags (
  transaction_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  PRIMARY KEY (transaction_id, tag_id),
  FOREIGN KEY (transaction_id) REFERENCES transactions (id),
  FOREIGN KEY (tag_id) REFERENCES tags (id)
) STRICT|}

    let create_account_tag =
      {|
CREATE TABLE account_tags (
  account_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  PRIMARY KEY (account_id, tag_id),
  FOREIGN KEY (account_id) REFERENCES accounts (id),
  FOREIGN KEY (tag_id) REFERENCES tags (id)
) STRICT|}

    let create_account_transactions_view account =
      (* FIXME: This query is insecure against SQL injection. *)
      Printf.sprintf
        {|
CREATE VIEW '%s' AS
WITH target_transaction_ids AS (
  SELECT p.transaction_id
  FROM postings p
  INNER JOIN full_accounts a ON p.account_id = a.id
  WHERE a.name = '%s'
), split_transaction_ids AS (
  SELECT p.transaction_id
  FROM postings p
  WHERE p.transaction_id IN ( SELECT * FROM target_transaction_ids )
  GROUP BY p.transaction_id
  HAVING COUNT(*) > 2
)
SELECT t.id AS id,
       t.created_at AS created_at,
       t.narration AS narration,
       p.narration AS p_narration,
       CASE a.name WHEN '%s' THEN '----- 諸口 -----' ELSE a.name END AS account,
       CASE a.name WHEN '%s' THEN p.amount ELSE -p.amount END AS amount,
       SUM(
         CASE a.name WHEN '%s' THEN p.amount ELSE -p.amount END
       ) OVER (ORDER BY t.created_at, t.id, p.id) AS balance
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
WHERE (t.id IN (SELECT * FROM split_transaction_ids) AND a.name = '%s')
OR    (t.id NOT IN (SELECT * FROM split_transaction_ids) AND
       t.id IN (SELECT * FROM target_transaction_ids) AND
       a.name <> '%s')
ORDER BY t.created_at DESC, t.id DESC, p.id DESC
|}
        account account account account account account account

    let insert_account =
      {|INSERT INTO accounts (id, name, currency, parent_id, kind) VALUES (?, ?, ?, ?, ?) RETURNING id|}

    let insert_transaction =
      {|INSERT INTO transactions (id, created_at, narration) VALUES (?, ?, ?) RETURNING id|}

    let insert_posting =
      {|INSERT INTO postings (id, account_id, transaction_id, amount, narration) VALUES (?, ?, ?, ?, ?)|}

    let insert_tag = {|INSERT INTO tags (id, name) VALUES (?, ?) RETURNING id|}

    let insert_transaction_tag =
      {|INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (?, ?)|}

    let insert_account_tag =
      {|INSERT INTO account_tags (account_id, tag_id) VALUES (?, ?)|}

    let select_account =
      {|SELECT id FROM accounts WHERE name = ? AND currency = ? AND parent_id IS ?|}
  end

  open Datastore

  let create_accounts (con : connection) =
    execute (prepare con Q.create_accounts) [] |> Result.get_ok

  let create_transactions (con : connection) =
    execute (prepare con Q.create_transactions) [] |> Result.get_ok

  let create_postings (con : connection) =
    execute (prepare con Q.create_postings) [] |> Result.get_ok

  let create_full_accounts_view (con : connection) =
    execute (prepare con Q.create_full_accounts_view) [] |> Result.get_ok

  let create_tags (con : connection) =
    execute (prepare con Q.create_tags) [] |> Result.get_ok

  let create_transaction_tags (con : connection) =
    execute (prepare con Q.create_transaction_tag) [] |> Result.get_ok

  let create_account_tags (con : connection) =
    execute (prepare con Q.create_account_tag) [] |> Result.get_ok

  let create_account_transactions_view (con : connection) ~account =
    execute (prepare con (Q.create_account_transactions_view account)) []
    |> Result.get_ok

  let insert_accounts (con : connection) recs =
    let stmt = prepare con Q.insert_account in
    recs
    |> List.iter (fun (id, name, currency, parent_id, kind) ->
           execute stmt
             [
               Int id;
               Text name;
               Text currency;
               parent_id |> Option.fold ~none:Null ~some:(fun x -> Int x);
               Int kind;
             ]
           |> Result.get_ok)

  let insert_transactions (con : connection) recs =
    let stmt = prepare con Q.insert_transaction in
    recs
    |> List.iter (fun (id, created_at, narration) ->
           execute stmt [ Int id; Text created_at; Text narration ]
           |> Result.get_ok)

  let insert_postings (con : connection) recs =
    let stmt = prepare con Q.insert_posting in
    recs
    |> List.iter (fun (id, account_id, transaction_id, amount, narration) ->
           execute stmt
             [
               Int id;
               Int account_id;
               Int transaction_id;
               Int amount;
               Text narration;
             ]
           |> Result.get_ok)

  let insert_tags (con : connection) recs =
    let stmt = prepare con Q.insert_tag in
    recs
    |> List.iter (fun (id, name) ->
           execute stmt [ Int id; Text name ] |> Result.get_ok)

  let insert_transaction_tags (con : connection) recs =
    let stmt = prepare con Q.insert_transaction_tag in
    recs
    |> List.iter (fun (transaction_id, tag_id) ->
           execute stmt [ Int transaction_id; Int tag_id ] |> Result.get_ok)

  let insert_account_tags (con : connection) recs =
    let stmt = prepare con Q.insert_account_tag in
    recs
    |> List.iter (fun (account_id, tag_id) ->
           execute stmt [ Int account_id; Int tag_id ] |> Result.get_ok)
end

module StringMap = Map.Make (String)

let rec list_last = function
  | [] -> failwith "list_last"
  | [ x ] -> x
  | _ :: xs -> list_last xs

let dump_account_id_map (model : Model.t) =
  (* (partial) account name -> int *)
  model.accounts
  |> List.map (fun (a : Model.open_account) -> a.account)
  |> List.flatten
  |> List.mapi (fun i n -> (n, i))
  |> List.sort_uniq (fun (l, _) (r, _) -> compare l r)
  |> List.to_seq |> StringMap.of_seq

let dump_tag_id_map (model : Model.t) =
  (model.accounts |> List.map (fun (a : Model.open_account) -> a.tags))
  @ (model.transactions |> List.map (fun (t : Model.transaction) -> t.tags))
  |> List.flatten |> List.sort_uniq compare
  |> List.mapi (fun i n -> (n, i))
  |> List.to_seq |> StringMap.of_seq

let dump_account_records (model : Model.t) account_id_map =
  (* (id, name, currency, parent_id, kind) list *)
  model.accounts
  |> List.map (fun (a : Model.open_account) ->
         a.account
         |> List.fold_left
              (fun (parent_id, recs) name ->
                let id = account_id_map |> StringMap.find name in
                ( Some id,
                  ( id,
                    name,
                    a.currency,
                    parent_id,
                    Model.int_of_account_kind a.kind )
                  :: recs ))
              (None, [])
         |> snd)
  |> List.flatten |> List.sort_uniq compare

let dump_tag_records (model : Model.t) tag_id_map =
  (* (id, name) list *)
  (model.accounts |> List.map (fun (a : Model.open_account) -> a.tags))
  @ (model.transactions |> List.map (fun (t : Model.transaction) -> t.tags))
  |> List.flatten |> List.sort_uniq compare
  |> List.map (fun tag -> (tag_id_map |> StringMap.find tag, tag))

let dump_account_tag_records (model : Model.t) account_id_map tag_id_map =
  (* (account_id, tag_id) list *)
  model.accounts
  |> List.map (fun (a : Model.open_account) ->
         let a_id = account_id_map |> StringMap.find (list_last a.account) in
         a.tags
         |> List.map (fun tag ->
                let tag_id = tag_id_map |> StringMap.find tag in
                (a_id, tag_id)))
  |> List.flatten |> List.sort_uniq compare

let dump_transaction_records_and_posting_records (model : Model.t)
    account_id_map =
  (* (id, date, narration) list *)
  (* (id, account_id, transaction_id, amount, narration) list *)
  model.transactions
  |> List.fold_left
       (fun (tx_recs, ps_recs) (tx : Model.transaction) ->
         let tx_id = List.length tx_recs in
         let tx_recs' =
           (tx_id, Store.string_of_date tx.date, tx.narration) :: tx_recs
         in
         let ps_recs' =
           (tx.postings
           |> List.mapi (fun i (p : Model.posting) ->
                  let ps_id = List.length ps_recs + i in
                  let account_name = list_last p.account in
                  let acc_id =
                    try account_id_map |> StringMap.find account_name
                    with Not_found ->
                      failwith
                        (Printf.sprintf "account not found: %s" account_name)
                  in
                  (ps_id, acc_id, tx_id, Option.get p.amount, p.narration)))
           @ ps_recs
         in
         (tx_recs', ps_recs'))
       ([], [])

let dump_transaction_tag_records (model : Model.t) tag_id_map =
  (* (transaction_id, tag_id) list *)
  model.transactions
  |> List.mapi (fun i (t : Model.transaction) ->
         t.tags |> List.map (fun tag -> (i, tag_id_map |> StringMap.find tag)))
  |> List.flatten |> List.sort_uniq compare

let dump' model pool =
  Datastore.use pool @@ fun con ->
  Store.create_accounts con;
  Store.create_transactions con;
  Store.create_postings con;
  Store.create_tags con;
  Store.create_transaction_tags con;
  Store.create_account_tags con;

  let account_id_map = dump_account_id_map model in
  let tag_id_map = dump_tag_id_map model in
  let account_records = dump_account_records model account_id_map in
  let tag_records = dump_tag_records model tag_id_map in
  let account_tag_records =
    dump_account_tag_records model account_id_map tag_id_map
  in
  let transaction_records, posting_records =
    dump_transaction_records_and_posting_records model account_id_map
  in
  let transaction_tag_records = dump_transaction_tag_records model tag_id_map in

  Store.insert_accounts con account_records;
  Store.insert_tags con tag_records;
  Store.insert_account_tags con account_tag_records;
  Store.insert_transactions con transaction_records;
  Store.insert_postings con posting_records;
  Store.insert_transaction_tags con transaction_tag_records;

  Store.create_full_accounts_view con;

  model.accounts
  |> List.iter @@ fun (acc : Model.open_account) ->
     Store.create_account_transactions_view con
       ~account:(String.concat ":" acc.account)

let dump path (model : Model.t) =
  let pool = Datastore.open_db path in
  dump' model pool >|= fun () -> pool

let with_dump_file (m : Model.t) f =
  Lwt_io.with_temp_dir ~prefix:"qash-" @@ fun tempdir ->
  dump (Filename.concat tempdir "db") m >>= f

module SQLX = Sqlx.Engine.Make (Sqlx.Driver_pg)

module Pg = struct
  module Q = struct
    let create_accounts =
      {|
CREATE TABLE IF NOT EXISTS accounts (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  currency TEXT NOT NULL,
  parent_id INTEGER,
  kind INTEGER NOT NULL,

  UNIQUE (name, currency, parent_id)
)|}

    let create_transactions =
      {|
CREATE TABLE IF NOT EXISTS transactions (
  id INTEGER PRIMARY KEY,
  created_at DATE NOT NULL,
  narration TEXT NOT NULL
)|}

    let create_postings =
      {|
CREATE TABLE IF NOT EXISTS postings (
  id INTEGER PRIMARY KEY,
  account_id INTEGER NOT NULL,
  transaction_id INTEGER NOT NULL,
  amount INTEGER NOT NULL,
  narration TEXT NOT NULL,
  cost INTEGER,
  price INTEGER,
  FOREIGN KEY (account_id) REFERENCES accounts (id),
  FOREIGN KEY (transaction_id) REFERENCES transactions (id)
)|}

    let create_full_accounts_view =
      {|
CREATE VIEW full_accounts AS
WITH RECURSIVE tree(id, name, currency, parent_id, kind, depth) AS (
  SELECT id, name, currency, parent_id, kind, 0 FROM accounts WHERE parent_id IS NULL
  UNION ALL
  SELECT a.id, t.name || ':' || a.name, a.currency, a.parent_id, a.kind, t.depth + 1 FROM accounts a JOIN tree t ON a.parent_id = t.id
)
SELECT id, name, currency, kind, depth FROM tree
|}

    let create_tags =
      {|
CREATE TABLE IF NOT EXISTS tags (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
)|}

    let create_transaction_tag =
      {|
CREATE TABLE IF NOT EXISTS transaction_tags (
  transaction_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  PRIMARY KEY (transaction_id, tag_id),
  FOREIGN KEY (transaction_id) REFERENCES transactions (id),
  FOREIGN KEY (tag_id) REFERENCES tags (id)
)|}

    let create_account_tag =
      {|
CREATE TABLE IF NOT EXISTS account_tags (
  account_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  PRIMARY KEY (account_id, tag_id),
  FOREIGN KEY (account_id) REFERENCES accounts (id),
  FOREIGN KEY (tag_id) REFERENCES tags (id)
)|}

    let create_account_transactions_view account =
      (* FIXME: This query is insecure against SQL injection. *)
      Printf.sprintf
        {|
CREATE VIEW "%s" AS
WITH RECURSIVE descendants AS (
  SELECT id FROM full_accounts WHERE name = '%s'
  UNION ALL
  SELECT a.id
  FROM accounts a
  INNER JOIN descendants d ON a.parent_id = d.id
), target_transaction_ids AS (
  SELECT p.transaction_id, a.name
  FROM postings p
  INNER JOIN full_accounts a ON p.account_id = a.id
  WHERE a.id IN (SELECT * FROM descendants)
), split_transaction_ids AS (
  SELECT p.transaction_id
  FROM postings p
  WHERE p.transaction_id IN (SELECT transaction_id FROM target_transaction_ids)
  GROUP BY p.transaction_id
  HAVING COUNT(*) > 2
), aggregated_transactions AS (
  SELECT
    t.id AS id,
    p.id AS p_id,
    t.created_at AS created_at,
    t.narration AS narration,
    p.narration AS p_narration,
    a.id IN (SELECT * FROM descendants) AS is_split,
    a.name AS a_name,
    tt.name AS tt_name,
    p.amount AS amount
  FROM postings p
  INNER JOIN full_accounts a ON p.account_id = a.id
  INNER JOIN transactions t ON p.transaction_id = t.id
  LEFT OUTER JOIN target_transaction_ids tt ON tt.transaction_id = t.id
  WHERE (t.id IN (SELECT * FROM split_transaction_ids) AND
         a.id IN (SELECT * FROM descendants))
  OR    (t.id NOT IN (SELECT * FROM split_transaction_ids) AND
         t.id IN (SELECT transaction_id FROM target_transaction_ids) AND
         a.id NOT IN (SELECT * FROM descendants))
)
SELECT
  at.id,
  at.created_at,
  at.narration,
  at.p_narration,
  CASE
    WHEN     at.is_split AND at.amount >= 0 THEN at.a_name
    WHEN     at.is_split AND at.amount <  0 THEN '----- 諸口 -----'
    WHEN NOT at.is_split AND at.amount >= 0 THEN at.a_name
    WHEN NOT at.is_split AND at.amount <  0 THEN at.tt_name
  END AS account1,
  CASE
    WHEN     at.is_split AND at.amount >= 0 THEN '----- 諸口 -----'
    WHEN     at.is_split AND at.amount <  0 THEN at.a_name
    WHEN NOT at.is_split AND at.amount >= 0 THEN at.tt_name
    WHEN NOT at.is_split AND at.amount <  0 THEN at.a_name
  END AS account2,
  ABS(at.amount) AS amount
FROM aggregated_transactions at
ORDER BY at.created_at DESC, at.id DESC, at.p_id DESC|}
        account account

    let insert_account =
      {|INSERT INTO accounts (id, name, currency, parent_id, kind) VALUES ($1, $2, $3, $4, $5)|}

    let insert_transaction =
      {|INSERT INTO transactions (id, created_at, narration) VALUES ($1, $2, $3)|}

    let insert_posting =
      {|INSERT INTO postings (id, account_id, transaction_id, amount, narration) VALUES ($1, $2, $3, $4, $5)|}

    let insert_tag = {|INSERT INTO tags (id, name) VALUES ($1, $2)|}

    let insert_transaction_tag =
      {|INSERT INTO transaction_tags (transaction_id, tag_id) VALUES ($1, $2)|}

    let insert_account_tag =
      {|INSERT INTO account_tags (account_id, tag_id) VALUES ($1, $2)|}

    let select_account =
      {|SELECT id FROM accounts WHERE name = $1 AND currency = $2 AND parent_id IS $3|}

    let drop_schema_public_cascade = {|DROP SCHEMA public CASCADE|}
    let create_schema_public = {|CREATE SCHEMA public|}

    let balance_function =
      {|
CREATE FUNCTION balance(var_account TEXT, var_depth INTEGER, var_timeFrom TIMESTAMP, var_timeTo TIMESTAMP)
RETURNS TABLE("time" TIMESTAMP, account_name TEXT, balance INTEGER)
AS $$
WITH RECURSIVE account_tree AS (
  -- 指定されたアカウントを取得（単純名またはfull_name）
  SELECT 
    a.id, 
    a.name, 
    a.currency, 
    a.parent_id, 
    a.kind,
    fa.name as full_name,
    fa.name as display_name,
    0 as depth
  FROM full_accounts fa
  INNER JOIN accounts a ON fa.id = a.id
  WHERE fa.name = var_account

  UNION ALL

  -- 子アカウントを再帰的に取得
  SELECT 
    a.id, 
    a.name, 
    a.currency, 
    a.parent_id, 
    a.kind,
    fa.name as full_name,
    CASE WHEN at.depth + 1 < var_depth
      THEN fa.name
      ELSE at.display_name
    END AS display_name,
    at.depth + 1
  FROM full_accounts fa
  INNER JOIN accounts a ON fa.id = a.id
  JOIN account_tree at ON a.parent_id = at.id
),

-- 対象アカウントの全ての取引を取得（全期間）
all_transactions AS (
  SELECT 
    t.created_at,
    at.display_name as account_name,
    p.amount
  FROM account_tree at
  JOIN postings p ON at.id = p.account_id
  JOIN transactions t ON p.transaction_id = t.id
),

-- 日付別・表示アカウント別に合計を計算
aggregated_transactions AS (
  SELECT 
    created_at,
    account_name,
    SUM(amount) as amount
  FROM all_transactions
  GROUP BY created_at, account_name
),

-- 各日付・アカウントの累積残高を計算（全期間）
all_running_balance AS (
  SELECT 
    created_at,
    account_name,
    SUM(amount) OVER (
      PARTITION BY account_name 
      ORDER BY created_at
      ROWS UNBOUNDED PRECEDING
    ) as balance
  FROM aggregated_transactions
),

-- 対象アカウント一覧
target_accounts AS (
  SELECT DISTINCT account_name
  FROM all_running_balance
),

-- 日付とアカウントの全組み合わせを取得する。
-- 前後一ヶ月余裕を持って取得することで、費用・収益の月次での計算を可能にする。
date_account_matrix AS (
  SELECT 
    dr.date AS created_at,
    ta.account_name
  FROM dates_daily(var_timeFrom-'1 month'::interval, var_timeTo+'1 month'::interval) dr
  CROSS JOIN target_accounts ta
),

-- 各日付・アカウントの最新残高を取得（前方埋め）
balance_with_forward_fill AS (
  SELECT 
    dam.created_at,
    dam.account_name,
    COALESCE(
      arb.balance,
      (
        SELECT arb2.balance
        FROM all_running_balance arb2
        WHERE arb2.account_name = dam.account_name
          AND arb2.created_at <= dam.created_at
        ORDER BY arb2.created_at DESC
        LIMIT 1
      ),
      0  -- アカウントに履歴がない場合は0
    ) as balance
  FROM date_account_matrix dam
  LEFT JOIN all_running_balance arb ON dam.created_at = arb.created_at 
                                   AND dam.account_name = arb.account_name
)

SELECT
  created_at AS time,
  account_name,
  balance
FROM balance_with_forward_fill
$$
LANGUAGE SQL|}

    let cashflow_balance_function =
      {|
CREATE FUNCTION cashflow_balance(var_depth integer, var_timeFrom timestamp, var_timeTo timestamp)
RETURNS TABLE("time" TIMESTAMP, "inout" TEXT, account_name TEXT, balance INTEGER)
AS $$
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 < var_depth THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
),
cash_account_ids AS (
  SELECT a.id
  FROM accounts a
  INNER JOIN account_tags r ON a.id = r.account_id
  INNER JOIN tags t ON r.tag_id = t.id
  WHERE t.name = '#cash'
),
target_postings AS (
  SELECT DISTINCT p2.*
  FROM postings p1, postings p2
  WHERE p1.transaction_id = p2.transaction_id
  AND p1.account_id IN ( SELECT * FROM cash_account_ids )
),
all_transactions AS (
  SELECT
    t.created_at,
    a.name AS account_name,
    CASE WHEN p.amount < 0 THEN 'in' ELSE 'out' END AS inout,
    p.amount
  FROM target_postings p
  INNER JOIN transactions t ON p.transaction_id = t.id
  INNER JOIN account_lifted al ON p.account_id = al.id
  INNER JOIN full_accounts a ON al.lifted = a.id
  WHERE p.account_id NOT IN ( SELECT * FROM cash_account_ids )
),
aggregated_transactions AS (
  SELECT 
    created_at,
    inout,
    account_name,
    SUM(amount) as amount
  FROM all_transactions
  GROUP BY created_at, inout, account_name
),
all_running_balance AS (
  SELECT 
    created_at,
    inout,
    account_name,
    SUM(amount) OVER (
      PARTITION BY inout, account_name 
      ORDER BY created_at
      ROWS UNBOUNDED PRECEDING
    ) as balance
  FROM aggregated_transactions
),
-- 対象アカウント一覧
target_accounts AS (
  SELECT DISTINCT inout, account_name
  FROM all_running_balance
),
-- 日付とアカウントの全組み合わせを取得する。
-- 前後一ヶ月余裕を持って取得することで、費用・収益の月次での計算を可能にする。
date_account_matrix AS (
  SELECT 
    dr.date AS created_at,
    ta.inout,
    ta.account_name
  FROM dates_daily(var_timeFrom-'1 month'::interval, var_timeTo+'1 month'::interval) dr
  CROSS JOIN target_accounts ta
),
-- 各日付・アカウントの最新残高を取得（前方埋め）
balance_with_forward_fill AS (
  SELECT 
    dam.created_at,
    dam.inout,
    dam.account_name,
    COALESCE(
      arb.balance,
      (
        SELECT arb2.balance
        FROM all_running_balance arb2
        WHERE arb2.inout = dam.inout
          AND arb2.account_name = dam.account_name
          AND arb2.created_at <= dam.created_at
        ORDER BY arb2.created_at DESC
        LIMIT 1
      ),
      0  -- アカウントに履歴がない場合は0
    ) as balance
  FROM date_account_matrix dam
  LEFT JOIN all_running_balance arb ON dam.created_at = arb.created_at 
                                   AND dam.inout = arb.inout
                                   AND dam.account_name = arb.account_name
)
SELECT
  created_at AS time,
  inout,
  account_name,
  balance
FROM balance_with_forward_fill bff
$$
LANGUAGE SQL|}

    let dates_daily_function =
      {|
CREATE FUNCTION dates_daily(var_timeFrom TIMESTAMP, var_timeTo TIMESTAMP)
RETURNS TABLE("date" DATE)
AS $$
SELECT
  generate_series(
    date_trunc('day', var_timeFrom),
    date_trunc('day', var_timeTo),
    '1 day'::interval
  )::date AS date
$$
LANGUAGE SQL|}

    let dates_monthly_function =
      {|
CREATE FUNCTION dates_monthly(var_timeFrom TIMESTAMP, var_timeTo TIMESTAMP)
RETURNS TABLE("date" DATE)
AS $$
SELECT
  generate_series(
    date_trunc('month', var_timeFrom),
    date_trunc('month', var_timeTo),
    '1 month'::interval
  )::date AS date
$$
LANGUAGE SQL|}
  end

  let dump dsn (model : Model.t) =
    let account_id_map = dump_account_id_map model in
    let tag_id_map = dump_tag_id_map model in
    let account_records = dump_account_records model account_id_map in
    let tag_records = dump_tag_records model tag_id_map in
    let account_tag_records =
      dump_account_tag_records model account_id_map tag_id_map
    in
    let transaction_records, posting_records =
      dump_transaction_records_and_posting_records model account_id_map
    in
    let transaction_tag_records =
      dump_transaction_tag_records model tag_id_map
    in

    SQLX.initialize dsn;
    Q.
      [
        drop_schema_public_cascade;
        create_schema_public;
        create_accounts;
        create_transactions;
        create_postings;
        create_tags;
        create_transaction_tag;
        create_account_tag;
      ]
    |> Lwt_list.iter_s (fun sql -> SQLX.e (fun conn -> conn#execute sql));%lwt

    account_records
    |> Lwt_list.iter_s (fun (id, name, currency, parent_id, kind) ->
           SQLX.e @@ fun conn ->
           conn#execute Q.insert_account
             ~p:
               [
                 `Int id;
                 `String name;
                 `String currency;
                 parent_id |> Option.fold ~none:`Null ~some:(fun x -> `Int x);
                 `Int kind;
               ]);%lwt

    tag_records
    |> Lwt_list.iter_s (fun (id, name) ->
           SQLX.e @@ fun conn ->
           conn#execute Q.insert_tag ~p:[ `Int id; `String name ]);%lwt

    account_tag_records
    |> Lwt_list.iter_s (fun (account_id, tag_id) ->
           SQLX.e @@ fun conn ->
           conn#execute Q.insert_account_tag ~p:[ `Int account_id; `Int tag_id ]);%lwt

    transaction_records
    |> Lwt_list.iter_s (fun (id, created_at, narration) ->
           SQLX.e @@ fun conn ->
           conn#execute Q.insert_transaction
             ~p:[ `Int id; `String created_at; `String narration ]);%lwt

    posting_records
    |> Lwt_list.iter_s
         (fun (id, account_id, transaction_id, amount, narration) ->
           SQLX.e @@ fun conn ->
           conn#execute Q.insert_posting
             ~p:
               [
                 `Int id;
                 `Int account_id;
                 `Int transaction_id;
                 `Int amount;
                 `String narration;
               ]);%lwt

    transaction_tag_records
    |> Lwt_list.iter_s (fun (transaction_id, tag_id) ->
           SQLX.e @@ fun conn ->
           conn#execute Q.insert_transaction_tag
             ~p:[ `Int transaction_id; `Int tag_id ]);%lwt

    (SQLX.e @@ fun conn -> conn#execute Q.create_full_accounts_view);%lwt

    model.accounts
    |> List.map (fun (acc : Model.open_account) ->
           match acc.account with
           | [ x ] -> [ [ x ]; acc.account ]
           | x :: y :: _ -> [ [ x ]; [ x; y ]; acc.account ]
           | _ -> [ acc.account ])
    |> List.flatten |> List.sort_uniq compare
    |> Lwt_list.iter_s (fun (acc : Model.account) ->
           SQLX.e @@ fun conn ->
           conn#execute
             (Q.create_account_transactions_view (String.concat ":" acc)));%lwt

    Q.
      [
        dates_daily_function;
        dates_monthly_function;
        balance_function;
        cashflow_balance_function;
      ]
    |> Lwt_list.iter_s (fun sql -> SQLX.e (fun conn -> conn#execute sql));%lwt

    Lwt.return_unit
end

let dump_pg = Pg.dump
