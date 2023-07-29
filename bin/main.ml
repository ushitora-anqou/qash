open Qash
open Util
open Lwt.Infix

let to_json filename =
  match Parser.parse_file filename with
  | Error s -> failwith s
  | Ok r ->
      Model.yojson_of_directives r |> Yojson.Safe.to_string |> print_endline

let of_json filename =
  try
    with_file filename @@ fun ic ->
    Yojson.Safe.from_channel ic
    |> Model.directives_of_yojson |> Model.string_of_directives |> print_endline
  with Ppx_yojson_conv_lib.Yojson_conv.Of_yojson_error (e, j) ->
    Printf.eprintf "JSON parse error: %s\n%s\n"
      (match e with Failure s -> s | _ -> Printexc.to_string e)
      (Yojson.Safe.pretty_to_string j)

let of_gnucash_csv transactions_csv_filename =
  with_file transactions_csv_filename @@ fun ic ->
  let csv = Csv.of_channel ic in
  let _ = Csv.next csv (* Skip header row *) in

  let date_re =
    Regex.e {|^([0-9][0-9][0-9][0-9])年([0-9][0-9])月([0-9][0-9])日$|}
  in
  let read_row row =
    let date = List.nth row 0 in
    let transaction_narration = List.nth row 3 in
    let posting_narration = List.nth row 8 in
    let account = List.nth row 9 |> Model.account_of_string in
    let amount = List.nth row 12 |> Model.amount_of_string in
    match Regex.match_ date_re date with
    | [
     [|
       _;
       Some { substr = year; _ };
       Some { substr = month; _ };
       Some { substr = day; _ };
     |];
    ] ->
        `Transaction
          Model.(
            make_transaction
              ~date:
                (make_date ~year:(int_of_string year)
                   ~month:(int_of_string month) ~day:(int_of_string day))
              ~narration:transaction_narration
              ~postings:
                [
                  make_posting ~account ~amount ~narration:posting_narration ();
                ]
              ())
    | _ -> `Posting Model.(make_posting ~account ~amount ())
  in
  let cur, acc =
    Csv.fold_left csv ~init:(None, []) ~f:(fun (cur, acc) row ->
        match read_row row with
        | exception e ->
            Printf.eprintf "Error: %s: %s\n" (Printexc.to_string e)
              (String.concat ", " row);
            (cur, acc)
        | `Transaction txn ->
            ( Some txn,
              cur
              |> Option.fold ~none:acc ~some:(fun x ->
                     Model.{ x with postings = List.rev x.postings } :: acc) )
        | `Posting posting -> (
            match cur with
            | None -> failwith "Invalid format; posting without transaction"
            | Some cur ->
                (Some { cur with postings = posting :: cur.postings }, acc)))
  in
  let transactions =
    cur
    |> Option.fold ~none:acc ~some:(fun x -> x :: acc)
    |> List.sort (fun Model.{ date = lhs; _ } Model.{ date = rhs; _ } ->
           Model.date_compare lhs rhs)
  in

  let open_accounts =
    transactions
    |> List.map (fun t ->
           let open Model in
           t.postings |> List.map (fun (p : posting) -> p.account))
    |> List.flatten |> List.sort_uniq compare
    |> List.map (fun account ->
           (* FIXME *)
           let open Model in
           let kind =
             match account with
             | "資産" :: _ -> Asset
             | "負債" :: _ -> Liability
             | "収益" :: _ -> Income
             | "費用" :: _ -> Expense
             | "資本" :: _ -> Equity
             | _ -> Asset
           in
           Model.make_open_account ~account ~currency:"JPY" ~kind ())
  in

  (open_accounts |> List.map (fun p -> Model.OpenAccount p))
  @ (transactions |> List.map (fun txn -> Model.Transaction txn))
  |> Model.string_of_directives |> print_endline

let check filename =
  let m, notes = Loader.load_file filename in
  match
    Lwt_main.run
      (Sql_writer.dump_on_memory m >>= fun con -> Verifier.verify con notes)
  with
  | Ok () ->
      Printf.printf "%d accounts\n%d transactions\n" (List.length m.accounts)
        (List.length m.transactions)
  | Error e -> Printf.eprintf "Error: %s\n" e

let dump in_filename out_filename =
  let m, _ = Loader.load_file in_filename in
  Lwt_main.run
  @@
  let%lwt _ =
    Sql_writer.dump
      ("sqlite3://" ^ Filename.concat (Sys.getcwd ()) out_filename)
      m
  in
  Lwt.return_unit

let serve =
  let interface, port =
    match
      Sys.getenv_opt "BIND"
      |> Option.value ~default:"127.0.0.1:8080"
      |> String.split_on_char ':'
    with
    | [ interface; port ] -> (interface, int_of_string port)
    | _ -> failwithf "Invalid BIND: %s" (Sys.getenv "BIND")
  in
  Html_server.serve ~interface ~port

let generate num_entries =
  Generator.generate_sample num_entries |> print_endline

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Debug);

  let open Cmdliner in
  Cmd.(
    group
      (info "qash" ~version:"0.1.0" ~doc:"A command-line accounting tool")
      [
        v (info "of-gnucash-csv")
          Term.(
            const of_gnucash_csv
            $ Arg.(
                required
                & pos 0 (some string) None
                & info ~docv:"TRANSACTIONS-CSV-FILE" []));
        v (info "of-json")
          Term.(
            const of_json
            $ Arg.(required & pos 0 (some string) None & info ~docv:"FILE" []));
        v (info "to-json")
          Term.(
            const to_json
            $ Arg.(required & pos 0 (some string) None & info ~docv:"FILE" []));
        v (info "check")
          Term.(
            const check
            $ Arg.(required & pos 0 (some string) None & info ~docv:"FILE" []));
        v (info "serve")
          Term.(
            const serve
            $ Arg.(
                required & pos 0 (some string) None & info ~docv:"IN-FILE" []));
        v (info "dump")
          Term.(
            const dump
            $ Arg.(
                required & pos 0 (some string) None & info ~docv:"IN-FILE" [])
            $ Arg.(
                required & pos 1 (some string) None & info ~docv:"OUT-FILE" []));
        v (info "generate")
          Term.(
            const generate
            $ Arg.(
                required & pos 0 (some int) None & info ~docv:"NUM-ENTRIES" []));
      ]
    |> eval)
  |> exit
