
## trusted local host 
context('Connection basics')
test_that('Sql server source is well created',{
  my_db1 <- src_sqlserver(dbname = "TEST_RSQLSERVER", trusted = TRUE)
  ## non trusted local host 
  my_db2 <- src_sqlserver(dbname = "TEST_RSQLSERVER", user='collateral',password='Kollat')
  expect_equal(src_tbls(my_db1),src_tbls(my_db2))
  ## url connection : TRUSTED
  my_db3 <- src_sqlserver(
    url = "Server=localhost;Database=TEST_RSQLSERVER;Trusted_Connection=True;")
  expect_equal(src_tbls(my_db2),src_tbls(my_db3))
  
  ## url connection : NO TRUSTED
  my_db4 <- src_sqlserver(
    url = "Server=localhost;Database=TEST_RSQLSERVER;User Id=collateral;
  Password=Kollat;")
  expect_equal(src_tbls(my_db2),src_tbls(my_db4))
  
})

## trusted local host 
context('Connection basics')
test_that('Sql server source is well created',{
  ss_db <- src_sqlserver(dbname = "TEST_RSQLSERVER", trusted = TRUE)
  ## non trusted local host 
  tbl(ss_db,'T_DATE')
  browser()
  expect_equal(src_tbls(my_db2),src_tbls(my_db4))
  
})

