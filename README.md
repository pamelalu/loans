## Goals

Write a program that consumes loans from the stream and assigns each loan to a facility while respecting each facility’s covenants.

## Input

An input data set will consist of four CSV files, describing the facilities, banks, covenants, and loans, respectively.

facilities.csv

Field  | Type | Description 
------ | ---- | ---------
bank_id  | integer  | The ID of the bank providing this facility. 
facility_id  | integer  | The ID of the facility. 
interest_rate | float | Between 0 and 1; the interest rate of this facility. 
amount | integer | The total capacity of the facility in cents. 


banks.csv

| Field  | Type | Description |
| ------ | ---- | ---------|
| bank_id  | integer  | The ID of the bank. |
| bank_name  | string  | The name of the bank. |
    
    
covenants.csv

| Field  | Type | Description |
| ------ | ---- | ---------|
| bank_id  | integer  | The ID of the bank requiring this covenant. |
| facility_id  | integer  | If present, denotes that this covenant applies to the facility with this ID; otherwise, this covenant applies to all of the bank’s facilities. |
| max_default_likelihood | float | If present, specifies the maximum allowed default rate for loans in the facility (or in the bank’s facilities). |
| amount | integer | The total capacity of the facility in cents. |
| banned_state | string | If present, indicates that loans in the facility (or in the bank’s facilities) may not originate from this state. |


loans.csv

| Field  | Type | Description |
| ------ | ---- | ---------|
| id  | integer  | The ID of the loan. Strictly increasing. |
| amount  | integer  | The size of the loan in cents. |
| interest_rate | float | Between 0 and 1; the interest rate of the loan. In this simplified model, the amount of money we earn from a loan (if it doesn’t default) is amount * interest_rate.|
| default_likelihood | float | Between 0 and 1; the probability that this loan will default.  |
| state | string | State where the loan originated. |

Calculating Loan Yields


The expected yield of a loan funded by a facility is the amount of interest that we expect to earn from the loan (taking into account the chance of a default), minus the expected loss from a default, minus the interest that we pay to to the bank to use their facility:
     expected_yield = (1 - default_likelihood) * loan_interest_rate * amount - default_likelihood * amount - facility_interest_rate * amount

## Deliverables
* Program should consume the input data and attempt to fund each loan with a facility. 
Unfunded loans are ignored by our system ­­ they will earn no interest, nor will they lose money if they default. 
* Program should be streaming, meaning it that it should process loans in the order that they are received and not use future loans to determine how the current loan should be funded.
* Program should produce two output files:

assignments.csv

| Field  | Type | Description |
| ------------- | ------------- |
| loan_id  | integer  | The ID of the loan. |
| facility_id  | integer  | If the loan is funded, the ID of its facility; otherwise, empty. |

yields.csv

| Field  | Type | Description |
| ------------- | ------------- |
| facility_id  | integer  | The ID of the facility. 
| expected_yield  | integer  | The expected yield of the facility, rounded to the nearest cent.|
