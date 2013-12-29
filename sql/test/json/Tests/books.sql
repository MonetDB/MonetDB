-- based on http://goessner.net/articles/JsonPath/
create table books(i integer, j json);

insert into books values( 1, ' { "store": {
    "book": [ 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      },
      { "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  }
}');

select * from books;

-- Queries to be compiled into SQL/JSON
-- all authors of single book in the single store
SELECT json.filter_all(j,'author') FROM (
	SELECT json.filter(j,'book') AS j FROM (
		SELECT json.filter(j,'store') AS j FROM books
	) AS L1
) AS L2;

-- a single author from the book store
SELECT json.filter(j,'author') FROM (
	SELECT json.filter(j,'book') AS j FROM (
		SELECT json.filter(j,'store') AS j FROM books
	) AS L1
) AS L2;

SELECT json.path(j,'..author') FROM books;
SELECT json.path(j,'store.book[*].author') FROM books;
SELECT json.path(j,'..bicycle.price') FROM books;
SELECT json.path(j,'store.book.author[1]') FROM books;

drop table books;
