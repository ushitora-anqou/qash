open Lwt.Infix
open Util

module Store = struct
  module Q = struct
    open Caqti_request.Infix
    open Caqti_type.Std

    let select_account_transactions =
      (string ->* tup3 int string (tup4 string string string (tup2 int int)))
        {|
WITH target_transaction_ids AS (
  SELECT p.transaction_id
  FROM postings p
  INNER JOIN full_accounts a ON p.account_id = a.id
  WHERE a.name = $1
), split_transaction_ids AS (
  SELECT p.transaction_id
  FROM postings p
  WHERE p.transaction_id IN ( SELECT * FROM target_transaction_ids )
  GROUP BY p.transaction_id
  HAVING COUNT(*) > 2
)
SELECT t.id, t.created_at, t.narration, p.narration,
       CASE a.name WHEN $1 THEN '-- スプリット取引 --' ELSE a.name END,
       CASE a.name WHEN $1 THEN p.amount ELSE -p.amount END,
       SUM(
         CASE a.name WHEN $1 THEN p.amount ELSE -p.amount END
       ) OVER (ORDER BY t.created_at, t.id, p.id)
FROM postings p
INNER JOIN full_accounts a ON p.account_id = a.id
INNER JOIN transactions t ON p.transaction_id = t.id
WHERE (t.id IN (SELECT * FROM split_transaction_ids) AND a.name = $1)
OR    (t.id NOT IN (SELECT * FROM split_transaction_ids) AND
       t.id IN (SELECT * FROM target_transaction_ids) AND
       a.name <> $1)
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
    | Error _ -> failwith "failed to decode transactions from db"
    | Ok (None, acc) -> Lwt.return acc
    | Ok (Some (_, cur), acc) -> Lwt.return (cur :: acc)

  let select_transactions (module Db : Caqti_lwt.CONNECTION) =
    decode_transactions (Db.fold Q.select_transactions) ()

  let select_account_transactions (module Db : Caqti_lwt.CONNECTION) account =
    decode_transactions (Db.fold Q.select_account_transactions) account

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
end

let jingoo_model_of_transactions account_kind rows =
  let open Jingoo in
  let open Jg_types in
  let open Model in
  let string_of_amount i =
    let rec aux s =
      if String.length s <= 3 then s
      else
        aux (String.sub s 0 (String.length s - 3))
        ^ ","
        ^ String.sub s (String.length s - 3) 3
    in
    aux (string_of_int i)
  in
  rows
  |> List.map @@ fun tx ->
     Tobj
       [
         ("date", Tstr (string_of_date tx.date));
         ("narration", Tstr tx.narration);
         ( "postings",
           Tlist
             (tx.postings
             |> List.map (fun (p : posting) ->
                    let balance =
                      match account_kind with
                      | Model.Asset | Expense -> p.balance
                      | Liability | Equity | Income -> -p.balance
                    in
                    Tobj
                      [
                        ("narration", Tstr p.narration);
                        ("account", Tstr (string_of_account p.account));
                        ("amount", Tint (Option.get p.amount));
                        ( "abs_amount_s",
                          Tstr
                            (p.amount |> Option.get |> abs |> string_of_amount)
                        );
                        ("balance", Tint balance);
                        ("balance_s", Tstr (string_of_amount balance));
                      ])) );
       ]

let generate_ok_html con =
  let%lwt model_gl =
    Store.select_transactions con >|= jingoo_model_of_transactions Model.Asset
  in

  let%lwt model_accounts =
    Store.select_accounts con
    >>= Lwt_list.map_s (fun (account, kind) ->
            Store.select_account_transactions con account
            >|= jingoo_model_of_transactions (Model.account_kind_of_int kind)
            >|= fun model -> (account, Jingoo.Jg_types.Tlist model))
  in

  let get_model ~account ~depth ~year kind column =
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
    let open Jingoo.Jg_types in
    let labels =
      iota 12
      |> List.map (fun i -> Tstr (Printf.sprintf "%d-%02d-01" year (i + 1)))
    in
    let data =
      raw_data
      |> List.map
           (fun
             ( account_name,
               (jan, feb, mar, (apr, may, jun, (jul, aug, sep, (oct, nov, dec))))
             )
           ->
             let aux x = match column with `Debt -> x | `Credit -> -x in
             ( account_name,
               Tlist
                 [
                   Tint (aux jan);
                   Tint (aux feb);
                   Tint (aux mar);
                   Tint (aux apr);
                   Tint (aux may);
                   Tint (aux jun);
                   Tint (aux jul);
                   Tint (aux aug);
                   Tint (aux sep);
                   Tint (aux oct);
                   Tint (aux nov);
                   Tint (aux dec);
                 ] ))
    in
    Lwt.return (Tobj [ ("labels", Tlist labels); ("data", Tobj data) ])
  in

  let%lwt model_asset =
    get_model ~account:Asset ~depth:1 ~year:2023 `Stock `Debt
  in
  let%lwt model_liability =
    get_model ~account:Liability ~depth:1 ~year:2023 `Stock `Credit
  in
  let%lwt model_expense =
    get_model ~account:Expense ~depth:1 ~year:2023 `Flow `Debt
  in
  let%lwt model_income =
    get_model ~account:Income ~depth:1 ~year:2023 `Flow `Credit
  in

  let models =
    Jingoo.Jg_types.
      [
        ("gl", Tlist model_gl);
        ("account", Tobj model_accounts);
        ("asset", model_asset);
        ("liability", model_liability);
        ("expense", model_expense);
        ("income", model_income);
      ]
  in
  with_file "lib/index.html.tpl" (fun f ->
      f |> In_channel.input_all |> Jingoo.Jg_template.from_string ~models)
  |> Lwt.return

let generate_error_html msg =
  let models = Jingoo.Jg_types.[ ("message", Tstr msg) ] in
  with_file "lib/error.html.tpl" (fun f ->
      f |> In_channel.input_all |> Jingoo.Jg_template.from_string ~models)
  |> Lwt.return

let generate_html' in_filename =
  let m, notes = Loader.load_file in_filename in
  let%lwt con = Sql_writer.dump_on_memory m in
  match%lwt Verifier.verify con notes with
  | Error s -> failwithf "Verification error: %s" s
  | Ok () ->
      let (module C) = con in
      Lwt.finalize (fun () -> generate_ok_html con) (fun () -> C.disconnect ())

let generate_html in_filename =
  try%lwt generate_html' in_filename
  with e ->
    let message = match e with Failure s -> s | _ -> Printexc.to_string e in
    generate_error_html message

let start_watching filepath streams =
  let%lwt inotify = Lwt_inotify.create () in
  let rec loop () =
    try%lwt
      Lwt_inotify.add_watch inotify filepath Inotify.[ S_Modify ] |> ignore_lwt;%lwt
      let%lwt _, _events, _, _ = Lwt_inotify.read inotify in
      Dream.info (fun m -> m "File updated");
      !streams |> Lwt_list.iter_p (fun stream -> Dream.send stream "reload");%lwt
      loop ()
    with e ->
      Dream.error (fun m -> m "Watching error: %s" (Printexc.to_string e));
      Lwt.return_unit
  in
  Lwt.return @@ Lwt.async loop

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
           ( Dream.get "/" @@ fun _ ->
             let%lwt html = generate_html in_filename in
             Dream.html html );
         ]
  in
  Lwt_main.run f
