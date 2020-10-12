***************************
Support for JSON in MonetDB
***************************

MonetDB supports columns having the JSON type.

Usage
=====

The JSON type is implemented as a subtype of ``VARCHAR``, that is guaranteed to
only contain valid JSON strings. For example

.. code:: sql

  CREATE TABLE json_example (c1 JSON NOT NULL);
  INSERT INTO json_example VALUES ('{"k1": 1, "k2": "foo", "k3": [1,2,3]}');

MonetDB provides an implementation of `JSONpath`_ as the means to interact with
JSON data.

.. _JSONpath: https://goessner.net/articles/JsonPath/

============================ =========================================================
``json.filter(J, Pathexpr)`` Extracts the component from ``J`` that satisfied the Pathexpr
``json.filter(J, Number)``   Extracts an indexed component from ``J``
``json.text()``
============================ =========================================================
