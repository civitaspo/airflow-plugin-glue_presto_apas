0.0.10 (2019-04-12)
===================

*  Add to check the table has the same rows as affected rows.

0.0.9 (2019-04-03)
==================

* Add a check whether objects are created after `INSERT ... SELECT ...` query.

0.0.8 (2019-04-02)
==================

* More logging in `PrestoHook`.
* Handle `NO RESULTS` error.

0.0.7 (2019-04-02)
==================

* Use `presto-python-client` instead of `PyHive/pypresto` experimentally.

0.0.6 (2019-04-02)
==================

* Use `PrestoHook#get_first` instead of `PrestoHook#run` because `PrestoHook#run` can be use `autocommit=False` context.

0.0.5 (2019-04-02)
==================

* Handle a error: `CREATE TABLE` is not applied.
* Use `PrestoHook#run` instead of `PrestoHook#get_first` for `{CREATE,DROP} {VIEW,TABLE}`

0.0.4 (2019-03-25)
==================

* Add `follow_location` option to `GlueAddPartitionOperator`.

0.0.3 (2019-03-19)
==================

* Support templates

0.0.2 (2019-03-18)
==================

* Support Python3.6

0.0.1 (2019-03-18)
==================

* First Release
