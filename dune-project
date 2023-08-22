(lang dune 3.8)

(name qash)

(generate_opam_files true)

(source
 (github ushitora-anqou/qash))

(authors "Ushitora Anqou")

(maintainers "Ushitora Anqou")

(license LICENSE)

(documentation https://github.com/ushitora-anqou/qash)

(package
 (name qash)
 (synopsis "A programming language for double-entry accounting")
 (description
  "A programming language for double-entry accounting, inspired by Beancount")
 (tags (accounting))
 (depends
  (alcotest :with-test)
  (ocamlformat
   (= 0.26.0))
  caqti
  caqti-driver-sqlite3
  caqti-lwt
  csv
  dream
  dune
  (inotify
   (= :os linux))
  logs
  lwt_ppx
  menhir
  ocaml
  pcre
  ppx_deriving
  (ppx_yojson_conv
   (<= v0.15.1))
  yojson))

(using menhir 2.1)
