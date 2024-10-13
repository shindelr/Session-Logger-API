--  Hacky "Truncate" for the table
delete from dbo.[Location]
where [LocationID] > 0

-- Insert starter data
insert dbo.[Location] (SpotName, BuoyNum, Lat, Long)
values
    ('Agate Beach', '46050', 44.674131, -124.063319),
    ('Otter Rock', '46050', 44.746325, -124.062164),
    ('South Beach', '46050', 44.600865, -124.066266)
    
select * from dbo.[Location]
