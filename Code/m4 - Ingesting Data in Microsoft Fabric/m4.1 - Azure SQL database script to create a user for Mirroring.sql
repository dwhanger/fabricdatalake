--Use [master] database

SELECT * FROM sys.dm_server_managed_identities
GO

CREATE LOGIN fabric_login WITH PASSWORD = 'SuperSecret!';
ALTER SERVER ROLE [##MS_ServerStateReader##] ADD MEMBER fabric_login;
GO

-- Switch to [CustomersDB] database

CREATE USER fabric_user FOR LOGIN fabric_login;
GRANT CONTROL TO fabric_user;