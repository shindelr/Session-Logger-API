--  Hacky "Truncate" for the LogUser table
delete from dbo.LogUser
where UserID > 0

-- Insert Erica and Robin into the LogUser table
insert dbo.LogUser (Username, Passkey, email)
    VALUES 
    ('roshindelman', 'b09bEb374d', 'roshindelman@gmail.com'),
    ('erica.loftin', '1234', 'erica.loftin17@gmail.com')

select * from dbo.LogUser
