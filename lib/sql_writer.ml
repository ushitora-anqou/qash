open Lwt.Infix
open Util

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

    let create_account_tag =
      (unit ->. unit)
        {|
CREATE TABLE account_tags (
  account_id INTEGER NOT NULL,
  tag_id INTEGER NOT NULL,
  FOREIGN KEY (account_id) REFERENCES accounts (id),
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

    let insert_account_tag =
      (tup2 int int ->. unit)
        {|INSERT INTO account_tags (account_id, tag_id) VALUES (?, ?)|}

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

  let create_account_tags (module Db : Caqti_lwt.CONNECTION) =
    Db.exec Q.create_account_tag () |> raise_if_error

  let create_account_transactions_view (module Db : Caqti_lwt.CONNECTION)
      ~account =
    Db.exec (Q.create_account_transactions_view account) () |> raise_if_error

  let insert_account (module Db : Caqti_lwt.CONNECTION) ~account ~currency ~kind
      =
    let parent_id = ref None in
    account
    |> Lwt_list.iter_s (fun name ->
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
           Lwt.return (parent_id := Some id));%lwt
    !parent_id |> Option.get |> Lwt.return

  let insert_transaction (module Db : Caqti_lwt.CONNECTION) ~date ~narration =
    Db.find Q.insert_transaction (string_of_date date, narration)
    |> raise_if_error

  let insert_transactions (module Db : Caqti_lwt.CONNECTION)
      (txs : Model.transaction list) =
    (* FIXME: SQL injection *)
    let query =
      txs
      |> List.map (fun (tx : Model.transaction) ->
             Printf.sprintf "('%s', '%s')" (string_of_date tx.date) tx.narration)
      |> String.concat " "
      |> Printf.sprintf
           {|INSERT INTO transactions (created_at, narration) VALUES %s RETURNING id|}
    in
    let open Caqti_request.Infix in
    let open Caqti_type.Std in
    Db.fold_s ((unit ->* int) query) (fun x xs -> Lwt.return_ok (x :: xs)) () []
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

  let insert_account_tag (module Db : Caqti_lwt.CONNECTION) ~account_id ~tag_id
      =
    Db.exec Q.insert_account_tag (account_id, tag_id) |> raise_if_error

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

module StringMap = Map.Make (String)

let rec list_last = function
  | [] -> failwith "list_last"
  | [ x ] -> x
  | _ :: xs -> list_last xs

let dump_account_id_map (model : Model.t) =
  (* (partial) account name -> int *)
  model.accounts
  |> List.map (fun (a : Model.open_account) -> a.account)
  |> List.flatten |> List.sort_uniq compare
  |> List.mapi (fun i n -> (n, i))
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
                (Some id, (id, name, a.currency, parent_id, a.kind) :: recs))
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
                  let acc_id =
                    account_id_map |> StringMap.find (list_last p.account)
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

let dump_csv (model : Model.t) =
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

  let oc = open_out "accounts.csv" in
  account_records
  |> List.iter (fun (id, name, currency, parent_id, kind) ->
         Printf.fprintf oc "%d,%s,%s,%s,%d\n" id name currency
           (parent_id
           |> Option.fold ~none:"NULL" ~some:(fun id -> string_of_int id))
           (Model.int_of_account_kind kind));
  close_out oc;

  let oc = open_out "tags.csv" in
  tag_records
  |> List.iter (fun (id, name) -> Printf.fprintf oc "%d,%s\n" id name);
  close_out oc;

  let oc = open_out "account_tags.csv" in
  account_tag_records
  |> List.iter (fun (account_id, tag_id) ->
         Printf.fprintf oc "%d,%d\n" account_id tag_id);
  close_out oc;

  let oc = open_out "transactions.csv" in
  transaction_records
  |> List.iter (fun (id, date, narration) ->
         Printf.fprintf oc "%d,%s,%s\n" id date narration);
  close_out oc;

  let oc = open_out "postings.csv" in
  posting_records
  |> List.iter (fun (id, account_id, transaction_id, amount, narration) ->
         Printf.fprintf oc "%d,%d,%d,%d,%s\n" id account_id transaction_id
           amount narration);
  close_out oc;

  let oc = open_out "transaction_tags.csv" in
  transaction_tag_records
  |> List.iter (fun (transaction_id, tag_id) ->
         Printf.fprintf oc "%d,%d\n" transaction_id tag_id);
  close_out oc;

  ()

let dump uri (model : Model.t) =
  let%lwt con = Caqti_lwt.connect (Uri.of_string uri) >>= Caqti_lwt.or_fail in
  Store.create_accounts con;%lwt
  Store.create_transactions con;%lwt
  Store.create_postings con;%lwt
  Store.create_tags con;%lwt
  Store.create_transaction_tags con;%lwt
  Store.create_account_tags con;%lwt

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

  (* Insert accounts *)
  let%lwt _ =
    let module C = (val con : Caqti_lwt.CONNECTION) in
    let open Caqti_request.Infix in
    let open Caqti_type.Std in
    C.exec
      ((unit ->. unit)
      @@ Printf.sprintf
           {|INSERT INTO accounts (id, name, currency, parent_id, kind) VALUES %s|}
           (account_records
           |> List.map (fun (id, name, currency, parent_id, kind) ->
                  Printf.sprintf "(%d, '%s', '%s', %s, %d)" id
                    (escape_single_quote name)
                    (escape_single_quote currency)
                    (parent_id
                    |> Option.fold ~none:"NULL" ~some:(fun i -> string_of_int i)
                    )
                    (Model.int_of_account_kind kind))
           |> String.concat ", "))
      ()
    |> Store.raise_if_error
  in

  (* Insert tags *)
  let%lwt _ =
    let module C = (val con : Caqti_lwt.CONNECTION) in
    let open Caqti_request.Infix in
    let open Caqti_type.Std in
    C.exec
      ((unit ->. unit)
      @@ Printf.sprintf {|INSERT INTO tags (id, name) VALUES %s|}
           (tag_records
           |> List.map (fun (id, name) -> Printf.sprintf "(%d, '%s')" id name)
           |> String.concat ", "))
      ()
    |> Store.raise_if_error
  in

  (* Insert account_tags *)
  let%lwt _ =
    let module C = (val con : Caqti_lwt.CONNECTION) in
    let open Caqti_request.Infix in
    let open Caqti_type.Std in
    C.exec
      ((unit ->. unit)
      @@ Printf.sprintf
           {|INSERT INTO account_tags (account_id, tag_id) VALUES %s|}
           (account_tag_records
           |> List.map (fun (account_id, tag_id) ->
                  Printf.sprintf "(%d, %d)" account_id tag_id)
           |> String.concat ", "))
      ()
    |> Store.raise_if_error
  in

  (* Insert transactions *)
  let%lwt _ =
    let module C = (val con : Caqti_lwt.CONNECTION) in
    let open Caqti_request.Infix in
    let open Caqti_type.Std in
    C.exec
      ((unit ->. unit)
      @@ Printf.sprintf
           {|INSERT INTO transactions (id, created_at, narration) VALUES %s|}
           (transaction_records
           |> List.map (fun (id, date, narration) ->
                  Printf.sprintf "(%d, '%s', '%s')" id date
                    (escape_single_quote narration))
           |> String.concat ", "))
      ()
    |> Store.raise_if_error
  in

  (* Insert postings *)
  let%lwt _ =
    let module C = (val con : Caqti_lwt.CONNECTION) in
    let open Caqti_request.Infix in
    let open Caqti_type.Std in
    C.exec
      ((unit ->. unit)
      @@ Printf.sprintf
           {|INSERT INTO postings (id, account_id, transaction_id, amount, narration) VALUES %s|}
           (posting_records
           |> List.map
                (fun (id, account_id, transaction_id, amount, narration) ->
                  Printf.sprintf "(%d, %d, %d, %d, '%s')" id account_id
                    transaction_id amount narration)
           |> String.concat ", "))
      ()
    |> Store.raise_if_error
  in

  (* Insert transaction_tags *)
  let%lwt _ =
    let module C = (val con : Caqti_lwt.CONNECTION) in
    let open Caqti_request.Infix in
    let open Caqti_type.Std in
    C.exec
      ((unit ->. unit)
      @@ Printf.sprintf
           {|INSERT INTO transaction_tags (transaction_id, tag_id) VALUES %s|}
           (transaction_tag_records
           |> List.map (fun (transaction_id, tag_id) ->
                  Printf.sprintf "(%d, %d)" transaction_id tag_id)
           |> String.concat ", "))
      ()
    |> Store.raise_if_error
  in

  Store.create_full_accounts_view con;%lwt

  (model.accounts
  |> Lwt_list.iter_s @@ fun (acc : Model.open_account) ->
     Store.create_account_transactions_view con
       ~account:(String.concat ":" acc.account));%lwt

  Lwt.return con

let dump_on_memory = dump "sqlite3::memory:"
