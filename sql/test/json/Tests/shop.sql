-- based on http://goessner.net/articles/JsonPath/
create table books(i integer, j json);

insert into books values( 1, ' { 
    "book": 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      }}');

insert into books values( 2, ' { 
    "book": 
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }}');

insert into books values( 3, ' { 
    "book": 
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      }}');

insert into books values( 3, ' { 
    "book": 
      { "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }}');

select * from books;

-- all authors of single book in the single store
SELECT json.filter(j,'book.author') FROM books;
SELECT json.text(json.filter(j,'book.author')) FROM books;

-- a single author from the book store
SELECT * FROM books 
WHERE json.text(json.filter(j,'book.author')) = 'Nigel Rees';
SELECT * FROM books 
WHERE json.text(json.filter(j,'..author')) = 'Nigel Rees';

-- numeric calculations
SELECT * FROM books 
WHERE json.number(json.filter(j,'book.price')) >= 12.99;
SELECT * FROM books 
WHERE json.number(json.filter(j,'..price')) >= 12.99;

-- text pattern search
SELECT * FROM books 
WHERE json.text(j) like '%Tolkien%';

drop table books;
