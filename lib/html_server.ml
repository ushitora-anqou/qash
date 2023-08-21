open Lwt.Infix
open Util

module Store = struct
  module Q = struct
    open Caqti_request.Infix
    open Caqti_type.Std

    let select_split_account_transactions =
      (string ->* tup3 int string (tup4 string string string (tup2 int int)))
        {|
WITH target_transaction_ids AS (
  SELECT p.transaction_id
  FROM postings p
  INNER JOIN full_accounts a ON p.account_id = a.id
  WHERE a.name = $1
)
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       COALESCE(SUM(p.amount) FILTER ( WHERE a.name = $1 ) OVER (ORDER BY t.created_at, t.id, p.id), 0)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
WHERE t.id IN (SELECT * FROM target_transaction_ids)
ORDER BY t.created_at, t.id, p.id
|}

    let select_transactions =
      (unit ->* tup3 int string (tup4 string string string (tup2 int int)))
        {|
SELECT t.id, t.created_at, t.narration, p.narration, a.name, p.amount,
       sum(p.amount) OVER (ORDER BY t.created_at, t.id, p.id)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
ORDER BY t.created_at, t.id, p.id
|}

    let select_accounts =
      (unit ->* tup2 string int)
        {|SELECT name, kind FROM full_accounts ORDER BY name|}

    let select_accounts_by_depth_name =
      (tup2 int string ->* string)
        {|
SELECT name FROM full_accounts
WHERE depth = ? AND name LIKE ?
ORDER BY name
|}

    let select_cumulative_sum_amount_by_depth_account_year =
      (tup3 int int int
      ->* tup2 string
            (tup4 int int int
               (tup4 int int int (tup4 int int int (tup3 int int int)))))
        {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= $1 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
),
const AS (
    SELECT
        CAST($3 AS TEXT) AS year,
        CAST($3 + 1 AS TEXT) AS next_year,
        CAST($3 - 1 AS TEXT) AS prev_year
)
SELECT DISTINCT
    a.name,
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-01-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-02-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-03-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-04-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-05-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-06-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-07-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-08-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-09-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-10-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-11-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE t.created_at < c.year||'-12-01' ) OVER ( PARTITION BY al.lifted ), 0)
FROM postings p, const c
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE a.kind = $2
|}

    let select_sum_amount_by_depth_account_year =
      (tup3 int int int
      ->* tup2 string
            (tup4 int int int
               (tup4 int int int (tup4 int int int (tup3 int int int)))))
        {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= $1 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
),
const AS (
    SELECT
        CAST($3 AS TEXT) AS year,
        CAST($3 + 1 AS TEXT) AS next_year,
        CAST($3 - 1 AS TEXT) AS prev_year
)
SELECT DISTINCT
    a.name,
    COALESCE(SUM(p.amount) FILTER ( WHERE c.prev_year||'-12-01' <= t.created_at AND t.created_at < c.year||'-01-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-01-01' <= t.created_at AND t.created_at < c.year||'-02-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-02-01' <= t.created_at AND t.created_at < c.year||'-03-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-03-01' <= t.created_at AND t.created_at < c.year||'-04-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-04-01' <= t.created_at AND t.created_at < c.year||'-05-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-05-01' <= t.created_at AND t.created_at < c.year||'-06-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-06-01' <= t.created_at AND t.created_at < c.year||'-07-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-07-01' <= t.created_at AND t.created_at < c.year||'-08-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-08-01' <= t.created_at AND t.created_at < c.year||'-09-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-09-01' <= t.created_at AND t.created_at < c.year||'-10-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-10-01' <= t.created_at AND t.created_at < c.year||'-11-01' ) OVER ( PARTITION BY al.lifted ), 0),
    COALESCE(SUM(p.amount) FILTER ( WHERE c.year     ||'-11-01' <= t.created_at AND t.created_at < c.year||'-12-01' ) OVER ( PARTITION BY al.lifted ), 0)
FROM postings p, const c
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE a.kind = $2
|}

    let select_cashflow_in_by_year_depth =
      (tup2 int int
      ->* tup2 string
            (tup4 int int int
               (tup4 int int int (tup4 int int int (tup3 int int int)))))
        {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= $2 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
),
const AS (
  SELECT
    CAST($1 AS TEXT) AS year,
    CAST($1 + 1 AS TEXT) AS next_year,
    CAST($1 - 1 AS TEXT) AS prev_year
),
cash_account_ids AS (
  SELECT a.id
  FROM accounts a
  INNER JOIN account_tags r ON a.id = r.account_id
  INNER JOIN tags t ON r.tag_id = t.id
  WHERE t.name = '#cash'
)
SELECT DISTINCT
  a.name,
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-01-01' <= t.created_at AND t.created_at < c.year     ||'-02-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-02-01' <= t.created_at AND t.created_at < c.year     ||'-03-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-03-01' <= t.created_at AND t.created_at < c.year     ||'-04-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-04-01' <= t.created_at AND t.created_at < c.year     ||'-05-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-05-01' <= t.created_at AND t.created_at < c.year     ||'-06-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-06-01' <= t.created_at AND t.created_at < c.year     ||'-07-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-07-01' <= t.created_at AND t.created_at < c.year     ||'-08-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-08-01' <= t.created_at AND t.created_at < c.year     ||'-09-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-09-01' <= t.created_at AND t.created_at < c.year     ||'-10-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-10-01' <= t.created_at AND t.created_at < c.year     ||'-11-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-11-01' <= t.created_at AND t.created_at < c.year     ||'-12-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0),
  -COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-12-01' <= t.created_at AND t.created_at < c.next_year||'-01-01' AND p.amount < 0) OVER ( PARTITION BY al.lifted ), 0)
FROM postings p, const c
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE p.account_id NOT IN ( SELECT * FROM cash_account_ids )
AND EXISTS (
  SELECT * FROM postings p1
  WHERE p1.transaction_id = p.transaction_id
  AND p1.account_id IN ( SELECT * FROM cash_account_ids )
)
|}

    let select_cashflow_out_by_year_depth =
      (tup2 int int
      ->* tup2 string
            (tup4 int int int
               (tup4 int int int (tup4 int int int (tup3 int int int)))))
        {|
WITH RECURSIVE account_lifted (id, depth, lifted) AS (
    SELECT id, 0, id FROM accounts WHERE parent_id IS NULL
    UNION ALL
    SELECT a.id, t.depth + 1, CASE WHEN t.depth + 1 <= $2 THEN a.id ELSE t.lifted END
    FROM accounts a INNER JOIN account_lifted t ON a.parent_id = t.id
),
const AS (
  SELECT
    CAST($1 AS TEXT) AS year,
    CAST($1 + 1 AS TEXT) AS next_year,
    CAST($1 - 1 AS TEXT) AS prev_year
),
cash_account_ids AS (
  SELECT a.id
  FROM accounts a
  INNER JOIN account_tags r ON a.id = r.account_id
  INNER JOIN tags t ON r.tag_id = t.id
  WHERE t.name = '#cash'
)
SELECT DISTINCT
  a.name,
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-01-01' <= t.created_at AND t.created_at < c.year     ||'-02-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-02-01' <= t.created_at AND t.created_at < c.year     ||'-03-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-03-01' <= t.created_at AND t.created_at < c.year     ||'-04-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-04-01' <= t.created_at AND t.created_at < c.year     ||'-05-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-05-01' <= t.created_at AND t.created_at < c.year     ||'-06-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-06-01' <= t.created_at AND t.created_at < c.year     ||'-07-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-07-01' <= t.created_at AND t.created_at < c.year     ||'-08-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-08-01' <= t.created_at AND t.created_at < c.year     ||'-09-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-09-01' <= t.created_at AND t.created_at < c.year     ||'-10-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-10-01' <= t.created_at AND t.created_at < c.year     ||'-11-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-11-01' <= t.created_at AND t.created_at < c.year     ||'-12-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0),
  COALESCE(SUM(p.amount) FILTER (WHERE c.year||'-12-01' <= t.created_at AND t.created_at < c.next_year||'-01-01' AND p.amount > 0) OVER ( PARTITION BY al.lifted ), 0)
FROM postings p, const c
INNER JOIN transactions t ON p.transaction_id = t.id
INNER JOIN account_lifted al ON p.account_id = al.id
INNER JOIN full_accounts a ON al.lifted = a.id
WHERE p.account_id NOT IN ( SELECT * FROM cash_account_ids )
AND EXISTS (
  SELECT * FROM postings p1
  WHERE p1.transaction_id = p.transaction_id
  AND p1.account_id IN ( SELECT * FROM cash_account_ids )
)
|}
  end

  let raise_if_error f =
    match%lwt f with
    | Ok x -> Lwt.return x
    | Error e -> failwith (Caqti_error.show e)

  let decode_transactions fold arg =
    let aux
        (tid, created_at, (t_narration, p_narration, a_name, (amount, balance)))
        (cur, acc) =
      let posting =
        Model.(
          make_posting ~narration:p_narration
            ~account:(account_of_string a_name) ~amount ~balance ())
      in
      let tx =
        Model.(
          make_transaction
            ~date:(date_of_string created_at)
            ~narration:t_narration ~postings:[ posting ] ())
      in
      match cur with
      | None -> (Some (tid, tx), acc)
      | Some (tid', cur) when tid = tid' ->
          let cur = Model.{ cur with postings = posting :: cur.postings } in
          (Some (tid, cur), acc)
      | Some (_, cur) -> (Some (tid, tx), cur :: acc)
    in
    match%lwt fold aux arg (None, []) with
    | Error e ->
        failwithf "failed to decode transactions from db: %s"
          (Caqti_error.show e)
    | Ok (None, acc) -> Lwt.return acc
    | Ok (Some (_, cur), acc) -> Lwt.return (cur :: acc)

  let select_transactions (module Db : Caqti_lwt.CONNECTION) =
    decode_transactions (Db.fold Q.select_transactions) ()

  let select_split_account_transactions (module Db : Caqti_lwt.CONNECTION)
      account =
    decode_transactions (Db.fold Q.select_split_account_transactions) account

  let select_accounts (module Db : Caqti_lwt.CONNECTION) =
    Db.fold Q.select_accounts List.cons () [] |> raise_if_error

  let select_accounts_by_depth_name (module Db : Caqti_lwt.CONNECTION) ~depth
      ~name =
    Db.fold Q.select_accounts_by_depth_name List.cons (depth, name) []
    |> raise_if_error

  let select_cumulative_sum_amount_by_depth_account_year
      (module Db : Caqti_lwt.CONNECTION) ~depth ~account ~year =
    Db.fold Q.select_cumulative_sum_amount_by_depth_account_year List.cons
      (depth, account, year) []
    |> raise_if_error

  let select_sum_amount_by_depth_account_year (module Db : Caqti_lwt.CONNECTION)
      ~depth ~account ~year =
    Db.fold Q.select_sum_amount_by_depth_account_year List.cons
      (depth, account, year) []
    |> raise_if_error

  let select_cashflow_in_by_year_depth (module Db : Caqti_lwt.CONNECTION) ~year
      ~depth =
    Db.fold Q.select_cashflow_in_by_year_depth List.cons (year, depth) []
    |> raise_if_error

  let select_cashflow_out_by_year_depth (module Db : Caqti_lwt.CONNECTION) ~year
      ~depth =
    Db.fold Q.select_cashflow_out_by_year_depth List.cons (year, depth) []
    |> raise_if_error
end

let json_of_transactions account_kind rows : Yojson.Safe.t =
  let open Model in
  rows
  |> List.map (fun tx : Yojson.Safe.t ->
         `Assoc
           [
             ("date", `String (string_of_date tx.date));
             ("narration", `String tx.narration);
             ( "postings",
               `List
                 (tx.postings
                 |> List.map (fun (p : posting) : Yojson.Safe.t ->
                        let balance =
                          match account_kind with
                          | Model.Asset | Expense -> p.balance
                          | Liability | Equity | Income -> -p.balance
                        in
                        `Assoc
                          [
                            ("narration", `String p.narration);
                            ("account", `String (string_of_account p.account));
                            ("amount", `Int (Option.get p.amount));
                            ("balance", `Int balance);
                          ])) );
           ])
  |> fun x -> `List x

let get_model_gl con =
  Store.select_transactions con >|= json_of_transactions Model.Asset

let get_model_accounts con =
  Store.select_accounts con
  >>= Lwt_list.map_s (fun (account, kind) ->
          Store.select_split_account_transactions con account
          >|= json_of_transactions (Model.account_kind_of_int kind)
          >|= fun model -> (account, model))
  >|= fun x -> `Assoc x

let decode_monthly_data
    (jan, feb, mar, (apr, may, jun, (jul, aug, sep, (oct, nov, dec)))) =
  [ jan; feb; mar; apr; may; jun; jul; aug; sep; oct; nov; dec ]

let format_monthly_data_for_json year raw_data : Yojson.Safe.t =
  let get_monthly_labels year =
    iota 12
    |> List.map (fun i -> `String (Printf.sprintf "%d-%02d-01" year (i + 1)))
  in
  let labels = get_monthly_labels year in
  let data =
    raw_data
    |> List.filter_map @@ fun (account_name, stack, data) ->
       if data |> List.for_all (( = ) 0) then None
       else
         `Assoc
           [
             ("account", `String account_name);
             ("stack", `String stack);
             ("data", `List (data |> List.map (fun x -> `Int x)));
           ]
         |> Option.some
  in
  `Assoc [ ("labels", `List labels); ("data", `List data) ]

let get_models_asset_liability_expense_income ~depth ~year con =
  let get_raw_data ~account ~depth ~year kind column =
    let account = Model.int_of_account_kind account in
    let%lwt raw_data =
      match kind with
      | `Stock ->
          Store.select_cumulative_sum_amount_by_depth_account_year con ~depth
            ~account ~year
      | `Flow ->
          Store.select_sum_amount_by_depth_account_year con ~depth ~account
            ~year
    in
    let aux x = match column with `Debt -> x | `Credit -> -x in
    raw_data
    |> List.map (fun (account_name, data) ->
           (account_name, "default", data |> decode_monthly_data |> List.map aux))
    |> Lwt.return
  in
  let%lwt asset =
    get_raw_data ~account:Asset ~depth ~year `Stock `Debt
    >|= format_monthly_data_for_json year
  in
  let%lwt liability =
    get_raw_data ~account:Liability ~depth ~year `Stock `Credit
    >|= format_monthly_data_for_json year
  in
  let%lwt expense =
    get_raw_data ~account:Expense ~depth ~year `Flow `Debt
    >|= format_monthly_data_for_json year
  in
  let%lwt income =
    get_raw_data ~account:Income ~depth ~year `Flow `Credit
    >|= format_monthly_data_for_json year
  in
  Lwt.return (asset, liability, expense, income)

let get_model_cashflow ~year ~depth con =
  let%lwt cashflow_in =
    Store.select_cashflow_in_by_year_depth ~year ~depth con
    >|= List.map (fun (account, data) ->
            (account, "in", decode_monthly_data data))
  in
  let%lwt cashflow_out =
    Store.select_cashflow_out_by_year_depth ~year ~depth con
    >|= List.map (fun (account, data) ->
            (account, "out", decode_monthly_data data))
  in
  let cashflow =
    let sum_in =
      cashflow_in
      |> List.fold_left
           (fun acc (_, _, xs) ->
             List.combine acc xs |> List.map (fun (x, y) -> x + y))
           [ 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0 ]
    in
    let sum_out =
      cashflow_out
      |> List.fold_left
           (fun acc (_, _, xs) ->
             List.combine acc xs |> List.map (fun (x, y) -> x + y))
           [ 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0; 0 ]
    in
    List.combine sum_in sum_out |> List.map (fun (x, y) -> x - y)
  in
  format_monthly_data_for_json year
    (cashflow_in @ cashflow_out @ [ ("net", "net", cashflow) ])
  |> Lwt.return

let get_models ~year ~depth con =
  let%lwt model_gl = get_model_gl con in
  let%lwt model_accounts = get_model_accounts con in
  let%lwt model_asset, model_liability, model_expense, model_income =
    get_models_asset_liability_expense_income ~depth ~year con
  in
  let%lwt model_asset100, model_liability100, model_expense100, model_income100
      =
    get_models_asset_liability_expense_income ~depth:100 ~year con
  in
  let%lwt model_cashflow = get_model_cashflow ~year ~depth con in
  let%lwt model_cashflow100 = get_model_cashflow ~year ~depth:100 con in

  Lwt.return
    [
      ("gl", model_gl);
      ("account", model_accounts);
      ("asset", model_asset);
      ("liability", model_liability);
      ("expense", model_expense);
      ("income", model_income);
      ("cashflow", model_cashflow);
      ("asset100", model_asset100);
      ("liability100", model_liability100);
      ("expense100", model_expense100);
      ("income100", model_income100);
      ("cashflow100", model_cashflow100);
    ]

let generate in_filename thn err =
  try%lwt
    let m, notes = Loader.load_file in_filename in
    let%lwt con = Sql_writer.dump_on_memory m in
    match%lwt Verifier.verify con notes with
    | Error s -> failwithf "Verification error: %s" s
    | Ok () ->
        let (module C) = con in
        Lwt.finalize (fun () -> thn con) (fun () -> C.disconnect ())
  with e ->
    let message = match e with Failure s -> s | _ -> Printexc.to_string e in
    err message

let generate_json in_filename =
  let aux_ok con = get_models ~year:2023 ~depth:1 con >|= fun xs -> `Assoc xs in
  let aux_err msg = `Assoc [ ("error", `String msg) ] |> Lwt.return in
  generate in_filename aux_ok aux_err

let start_watching filepath streams =
  try%lwt
    Fsnotify.start_watching ~filepath (fun _ ->
        Dream.info (fun m -> m "File updated");
        !streams |> Lwt_list.iter_p (fun stream -> Dream.send stream "reload"))
  with Failure s ->
    Dream.error (fun m -> m "Watching error: %s" s);
    Lwt.return_unit

let serve ?(interface = "127.0.0.1") ?(port = 8080) in_filename =
  let streams = ref [] in
  let finalize_websocket_stream ws () =
    let%lwt _ = Dream.receive ws in
    Dream.close_websocket ws;%lwt
    streams := List.filter (( != ) ws) !streams;
    Dream.info (fun m -> m "WebSocket stream closed");
    Lwt.return_unit
  in
  let f =
    start_watching in_filename streams;%lwt
    Dream.info (fun m -> m "HTTP server started: %s:%d" interface port);
    Dream.serve ~interface ~port
    @@ Dream.logger
    @@ Dream.router
         [
           ( Dream.get "/ws" @@ fun _request ->
             Dream.websocket ~close:false (fun ws ->
                 streams := ws :: !streams;
                 Lwt.async (finalize_websocket_stream ws);
                 Lwt.return_unit) );
           ( Dream.get "/data.json" @@ fun _ ->
             generate_json in_filename >|= Yojson.Safe.to_string
             >>= Dream.json ~headers:[ ("Access-Control-Allow-Origin", "*") ] );
         ]
  in
  Lwt_main.run f
