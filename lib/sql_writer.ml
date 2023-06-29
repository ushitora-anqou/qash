open Lwt.Infix

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
WITH RECURSIVE tree(id, name, currency, parent_id, depth) AS (
  SELECT id, name, currency, parent_id, 0 FROM accounts WHERE parent_id IS NULL
  UNION ALL
  SELECT a.id, t.name || ':' || a.name, a.currency, a.parent_id, t.depth + 1 FROM accounts a JOIN tree t ON a.parent_id = t.id
)
SELECT id, name, currency, depth FROM tree
|}

    let insert_account =
      (tup3 string string (option int) ->! int)
        {|INSERT INTO accounts (name, currency, parent_id) VALUES (?, ?, ?) RETURNING id|}

    let insert_transaction =
      (tup2 string string ->! int)
        {|INSERT INTO transactions (created_at, narration) VALUES (?, ?) RETURNING id|}

    let insert_posting =
      (tup4 int int int string ->. unit)
        {|INSERT INTO postings (account_id, transaction_id, amount, narration) VALUES (?, ?, ?, ?)|}

    let select_account =
      (tup3 string string (option int) ->? int)
        {|SELECT id FROM accounts WHERE name = ? AND currency = ? AND parent_id IS ?|}

    let select_account_by_fullname =
      (string ->! int) {|SELECT id FROM full_accounts WHERE name = ?|}
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

  let insert_account (module Db : Caqti_lwt.CONNECTION) ~account ~currency =
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
             Db.find Q.insert_account (name, currency, !parent_id)
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

  let select_account_by_fullname (module Db : Caqti_lwt.CONNECTION) account =
    Db.find Q.select_account_by_fullname (string_of_account account)
    |> raise_if_error
end

let dump uri (model : Model.t) =
  let%lwt con = Caqti_lwt.connect (Uri.of_string uri) >>= Caqti_lwt.or_fail in
  Store.create_accounts con;%lwt
  Store.create_transactions con;%lwt
  Store.create_postings con;%lwt

  (model.accounts
  |> Lwt_list.iter_s @@ fun (acc : Model.open_account) ->
     Store.insert_account con ~account:acc.account ~currency:acc.currency);%lwt

  Store.create_full_accounts_view con;%lwt

  (model.transactions
  |> Lwt_list.iter_s @@ fun (tx : Model.transaction) ->
     let%lwt tx_id =
       Store.insert_transaction con ~date:tx.date ~narration:tx.narration
     in
     tx.postings
     |> Lwt_list.iter_s @@ fun (p : Model.posting) ->
        let%lwt account_id = Store.select_account_by_fullname con p.account in
        Store.insert_posting con ~account_id ~transaction_id:tx_id
          ~amount:p.amount ~narration:p.narration);%lwt

  Lwt.return con
