open Lwt.Infix
open Util

module Store = struct
  let string_of_date (x : Model.date) =
    Printf.sprintf "%04d-%02d-%02d" x.year x.month x.day

  let string_of_account account = String.concat ":" account

  module Q = struct
    open Caqti_request.Infix
    open Caqti_type.Std

    let create_accounts =
      (unit ->. unit)
        {|
CREATE TABLE accounts (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  currency TEXT NOT NULL,
  parent_id INTEGER,
  kind INTEGER NOT NULL,

  UNIQUE (name, currency, parent_id)
)|}

    let create_transactions =
      (unit ->. unit)
        {|
CREATE TABLE transactions (
  id INTEGER PRIMARY KEY,
  created_at TEXT NOT NULL,
  narration TEXT NOT NULL
)|}

    let create_postings =
      (unit ->. unit)
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
)|}

    let create_full_accounts_view =
      (unit ->. unit)
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
      (unit ->. unit)
        {|
CREATE TABLE tags (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL UNIQUE
)|}

    let create_transaction_tag =
      (unit ->. unit)
        {|
CREATE TABLE transaction_tags (
  transaction_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  FOREIGN KEY (transaction_id) REFERENCES transactions (id),
  FOREIGN KEY (tag_id) REFERENCES tags (id)
)|}

    let create_account_transactions_view account =
      (* FIXME: This query is insecure against SQL injection. *)
      (unit ->. unit)
      @@ Printf.sprintf
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
       CASE a.name WHEN '%s' THEN '-- スプリット取引 --' ELSE a.name END AS account,
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
      (tup4 string string (option int) int ->! int)
        {|INSERT INTO accounts (name, currency, parent_id, kind) VALUES (?, ?, ?, ?) RETURNING id|}

    let insert_transaction =
      (tup2 string string ->! int)
        {|INSERT INTO transactions (created_at, narration) VALUES (?, ?) RETURNING id|}

    let insert_posting =
      (tup4 int int int string ->. unit)
        {|INSERT INTO postings (account_id, transaction_id, amount, narration) VALUES (?, ?, ?, ?)|}

    let insert_tag =
      (string ->! int) {|INSERT INTO tags (name) VALUES (?) RETURNING id|}

    let insert_transaction_tag =
      (tup2 int int ->. unit)
        {|INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (?, ?)|}

    let select_account =
      (tup3 string string (option int) ->? int)
        {|SELECT id FROM accounts WHERE name = ? AND currency = ? AND parent_id IS ?|}

    let select_account_by_fullname =
      (string ->! int) {|SELECT id FROM full_accounts WHERE name = ?|}

    let select_tag = (string ->! int) {|SELECT id FROM tags WHERE name = ?|}
  end

  let raise_if_error f =
    match%lwt f with
    | Ok x -> Lwt.return x
    | Error e -> failwith (Caqti_error.show e)

  let create_accounts (module Db : Caqti_lwt.CONNECTION) =
    Db.exec Q.create_accounts () |> raise_if_error

  let create_transactions (module Db : Caqti_lwt.CONNECTION) =
    Db.exec Q.create_transactions () |> raise_if_error

  let create_postings (module Db : Caqti_lwt.CONNECTION) =
    Db.exec Q.create_postings () |> raise_if_error

  let create_full_accounts_view (module Db : Caqti_lwt.CONNECTION) =
    Db.exec Q.create_full_accounts_view () |> raise_if_error

  let create_tags (module Db : Caqti_lwt.CONNECTION) =
    Db.exec Q.create_tags () |> raise_if_error

  let create_transaction_tags (module Db : Caqti_lwt.CONNECTION) =
    Db.exec Q.create_transaction_tag () |> raise_if_error

  let create_account_transactions_view (module Db : Caqti_lwt.CONNECTION)
      ~account =
    Db.exec (Q.create_account_transactions_view account) () |> raise_if_error

  let insert_account (module Db : Caqti_lwt.CONNECTION) ~account ~currency ~kind
      =
    let parent_id = ref None in
    account
    |> Lwt_list.iter_s @@ fun name ->
       let%lwt id =
         match%lwt
           Db.find_opt Q.select_account (name, currency, !parent_id)
           |> raise_if_error
         with
         | Some id -> Lwt.return id
         | None ->
             Db.find Q.insert_account (name, currency, !parent_id, kind)
             |> raise_if_error
       in
       Lwt.return (parent_id := Some id)

  let insert_transaction (module Db : Caqti_lwt.CONNECTION) ~date ~narration =
    Db.find Q.insert_transaction (string_of_date date, narration)
    |> raise_if_error

  let insert_posting (module Db : Caqti_lwt.CONNECTION) ~account_id
      ~transaction_id ~amount ~narration =
    Db.exec Q.insert_posting (account_id, transaction_id, amount, narration)
    |> raise_if_error

  let insert_tag (module Db : Caqti_lwt.CONNECTION) ~name =
    Db.find Q.insert_tag name |> raise_if_error

  let insert_transaction_tag (module Db : Caqti_lwt.CONNECTION) ~transaction_id
      ~tag_id =
    Db.exec Q.insert_transaction_tag (transaction_id, tag_id) |> raise_if_error

  let select_tag (module Db : Caqti_lwt.CONNECTION) ~name =
    Db.find Q.select_tag name |> raise_if_error

  let select_account_by_fullname (module Db : Caqti_lwt.CONNECTION) account =
    match%lwt
      Db.find Q.select_account_by_fullname (string_of_account account)
    with
    | Ok x -> Lwt.return x
    | Error e ->
        failwithf "Couldn't select account by fullname: %s: %s"
          (string_of_account account)
          (Caqti_error.show e)
end

let dump uri (model : Model.t) =
  let%lwt con = Caqti_lwt.connect (Uri.of_string uri) >>= Caqti_lwt.or_fail in
  Store.create_accounts con;%lwt
  Store.create_transactions con;%lwt
  Store.create_postings con;%lwt
  Store.create_tags con;%lwt
  Store.create_transaction_tags con;%lwt

  (model.accounts
  |> Lwt_list.iter_s @@ fun (acc : Model.open_account) ->
     Store.insert_account con ~account:acc.account ~currency:acc.currency
       ~kind:(Model.int_of_account_kind acc.kind));%lwt

  Store.create_full_accounts_view con;%lwt

  (model.transactions
  |> Lwt_list.iter_s @@ fun (tx : Model.transaction) ->
     let%lwt tx_id =
       Store.insert_transaction con ~date:tx.date ~narration:tx.narration
     in

     tx.tags
     |> Lwt_list.map_s (fun name ->
            try%lwt Store.insert_tag con ~name
            with _ -> Store.select_tag con ~name)
     >>= Lwt_list.iter_s (fun tag_id ->
             Store.insert_transaction_tag con ~transaction_id:tx_id ~tag_id);%lwt

     tx.postings
     |> Lwt_list.iter_s @@ fun (p : Model.posting) ->
        let%lwt account_id = Store.select_account_by_fullname con p.account in
        Store.insert_posting con ~account_id ~transaction_id:tx_id
          ~amount:(Option.get p.amount) ~narration:p.narration);%lwt

  (model.accounts
  |> Lwt_list.iter_s @@ fun (acc : Model.open_account) ->
     Store.create_account_transactions_view con
       ~account:(String.concat ":" acc.account));%lwt

  Lwt.return con

let dump_on_memory = dump "sqlite3::memory:"
