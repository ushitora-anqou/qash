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
