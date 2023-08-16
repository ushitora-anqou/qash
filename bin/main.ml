open Qash
open Command

let () =
  Logs.set_reporter (Logs_fmt.reporter ());
  Logs.set_level (Some Logs.Info);

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
