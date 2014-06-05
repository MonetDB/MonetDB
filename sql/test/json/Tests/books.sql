-- based on http://goessner.net/articles/JsonPath/
create table books(i integer, j json);

insert into books values( 1, ' { "store": {
    "books": [ 
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
SELECT json.filter(j,'store.books.[1]..author') FROM books;

-- a single author from the book store
SELECT json.filter(j,'..books..author[1]') AS j FROM (
	SELECT json.filter(j,'store') AS j FROM books
) AS L1;

SELECT json.filter(j,'..author') FROM books;
SELECT json.filter(j,'store.books[*]..author') FROM books;
SELECT json.filter(j,'store.books..author') FROM books;
SELECT json.filter(j,'store.books..author[1]') FROM books;
SELECT json.filter(j,'..bicycle.price') FROM books;

drop table books;
