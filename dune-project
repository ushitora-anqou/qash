(lang dune 3.8)

(name qash)

(generate_opam_files true)

(source
 (github username/reponame))

(authors "Author Name")

(maintainers "Maintainer Name")

(license LICENSE)

(documentation https://url/to/documentation)

(package
 (name qash)
 (synopsis "A short synopsis")
 (description "A longer description")
 (tags
  (topics "to describe" your project))
 (depends
  alcotest
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
  (ppx_yojson_conv
   (<= v0.15.1))
  yojson))

(using menhir 2.1)
