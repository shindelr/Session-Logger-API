-- Session data insertion
-- Create a new stored procedure called 'session_data' in schema 'dbo'
-- Drop the stored procedure if it already exists
IF EXISTS (
SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES
WHERE SPECIFIC_SCHEMA = N'dbo'
    AND SPECIFIC_NAME = N'session_data'
    AND ROUTINE_TYPE = N'PROCEDURE'
)
DROP PROCEDURE dbo.session_data
GO
-- Create the stored procedure in the specified schema
CREATE PROCEDURE dbo.session_data
    -- Location
    @SpotName varchar(200),

    -- Session
    @Date date,
    @TimeIn time,
    @TimeOut time,
    @Rating int,

    -- User
    @Username varchar(150),

    -- Temps
    @ATemp float,
    @WTemp float,

    -- Swell
    @MeanWaveDir Int,
    @MeanWaveDirCard varchar(5),
    @MeanWaveHeight float,
    @DomPeriod float,

    -- Wind
    @MeanWindDir Int,
    @MeanWindDirCard varchar(5),
    @MeanWindSpeed float,
    @GustSpeed float,

    -- Tide parameters not implemented yet.
    @TideIncoming Bit = Null,
    @TideMaxHeight float = Null,
    @TideMinHeight float = Null
AS
BEGIN
    -- Insert individual instances of each entity
    insert into dbo.Temps (AirTemp, WaterTemp)
    values (@ATemp, @WTemp)

    insert into dbo.Swell (MeanWaveDir, MeanWaveDirCardinal, MeanWaveHeight, DomPeriod)
    values (@MeanWaveDir, @MeanWaveDirCard, @MeanWaveHeight, @DomPeriod)

    -- Should insert all Nulls into tides for now
    insert into dbo.Tide (Incoming, MaximumHeight, MinimumHeight)
    values(@TideIncoming, @TideMaxHeight, @TideMinHeight)

    insert into dbo.Wind (MeanWindDir, MeanWindDirCardinal, MeanWindSpeed, GustSpeed)
    values(@MeanWindDir, @MeanWindDirCard, @MeanWindSpeed, @GustSpeed)

    -- Insert the session last, using the keys of all the other entities created
    insert into dbo.SessionInfo (LocID, TempID, SwellID, TideID, WindID, UserID, Date, TimeIn, TimeOut, Rating)
    VALUES(
        (SELECT LocationID FROM dbo.Location WHERE SpotName = @SpotName),
        (Select TempID from dbo.temps 
            where AirTemp = @ATemp and 
            WaterTemp = @WTemp),
        (select SwellID from dbo.Swell 
            where MeanWaveDir = @MeanWaveDir and 
            MeanWaveDirCardinal = @MeanWaveDirCard and
            MeanWaveHeight = @MeanWaveHeight and
            DomPeriod = @DomPeriod),
        (select TideID from dbo.Tide 
            where Incoming = @TideIncoming and
            MaximumHeight = @TideMaxHeight and
            MinimumHeight = @TideMinHeight),
        (select WindID from dbo.Wind 
            where MeanWindDir = @MeanWindDir and
            MeanWindDirCardinal = @MeanWindDirCard and
            MeanWindSpeed = @MeanWindSpeed and
            GustSpeed = @GustSpeed)
    )
END
GO

-- -- example to execute the stored procedure we just created
-- EXECUTE dbo.session_data 1 /*value_for_param1*/, 2 /*value_for_param2*/
-- GO

-- Data: {'spot': 'Agate Beach', 'timeIn': '13:00', 'timeOut': '13:45', 'rating': 2}
-- Returning: {'WDIR': 358.0, 'WSPD': 22.8, 'GST': 29.5, 'WDIR_CARD': 'NW'}