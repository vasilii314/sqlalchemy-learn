CREATE TABLE employee (
	emp_id SERIAL PRIMARY KEY,
	emp_name VARCHAR
);

CREATE TABLE employee_of_month (
	emp_id SERIAL PRIMARY KEY,
	emp_name VARCHAR
);

INSERT INTO employee (emp_name) VALUES
('spongebob'),
('sandy'),
('squidward');