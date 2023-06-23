open Qash
open Util

let to_json filename =
  match Parser.parse_file filename with
  | Error s -> failwith s
  | Ok r ->
      Model.yojson_of_directives r |> Yojson.Safe.to_string |> print_endline

let of_json filename =
  with_file filename @@ fun ic ->
  Yojson.Safe.from_channel ic
  |> Model.directives_of_yojson |> Model.string_of_directives |> print_endline

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
           Model.make_open_account ~account ~currency:"JPY")
  in

  (open_accounts |> List.map (fun p -> Model.OpenAccount p))
  @ (transactions |> List.map (fun txn -> Model.Transaction txn))
  |> Model.string_of_directives |> print_endline

let check filename =
  let m = Loader.load_file filename in
  Printf.printf "%d accounts\n%d transactions\n" (List.length m.accounts)
    (List.length m.transactions)

let dump in_filename out_filename =
  let m = Loader.load_file in_filename in
  Sql_writer.dump
    ("sqlite3://" ^ Filename.concat (Sys.getcwd ()) out_filename)
    m
  |> Lwt_main.run

let () =
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
        v (info "dump")
          Term.(
            const dump
            $ Arg.(
                required & pos 0 (some string) None & info ~docv:"IN-FILE" [])
            $ Arg.(
                required & pos 1 (some string) None & info ~docv:"OUT-FILE" []));
      ]
    |> eval)
  |> exit
