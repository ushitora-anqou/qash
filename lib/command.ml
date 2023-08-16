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

let check filename =
  try
    let m, notes = Loader.load_file filename in
    match
      Lwt_main.run
        (Sql_writer.dump_on_memory m >>= fun con -> Verifier.verify con notes)
    with
    | Ok () ->
        Printf.printf "%d accounts\n%d transactions\n" (List.length m.accounts)
          (List.length m.transactions)
    | Error e -> Printf.eprintf "Error: %s\n" e
  with Failure s -> Printf.eprintf "Error: %s\n" s

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
