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
  Lwt_main.run
    (try%lwt
       let%lwt m, notes = Loader.load_file filename in
       Lwt_io.with_temp_dir ~prefix:"qash-" @@ fun tempdir ->
       match%lwt
         Sql_writer.dump (Filename.concat tempdir "db") m >>= fun con ->
         Verifier.verify con notes
       with
       | Ok () ->
           Printf.printf "%d accounts\n%d transactions\n"
             (List.length m.accounts)
             (List.length m.transactions);
           Lwt.return_unit
       | Error e ->
           Printf.eprintf "Error: %s\n" e;
           Lwt.return_unit
     with Failure s ->
       Printf.eprintf "Error: %s\n" s;
       Lwt.return_unit)

let dump in_filename out_filename =
  Lwt_main.run
  @@
  let%lwt m, _ = Loader.load_file in_filename in
  let filepath = Filename.concat (Sys.getcwd ()) out_filename in
  Sql_writer.dump filepath m >|= fun _ -> ()

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
  Web_server.serve ~interface ~port

let generate num_entries =
  Generator.generate_sample num_entries |> print_endline
