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
    @Username varchar(150) = Null,

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
    -- Declare variables to hold the IDs of the entities we are inserting
    DECLARE @TempID int, @SwellID int, @TideID int, @WindID int

    -- Insert individual instances of each entity
    insert into dbo.Temps (AirTemp, WaterTemp)
    values (@ATemp, @WTemp)
    set @TempID = SCOPE_IDENTITY()

    insert into dbo.Swell (MeanWaveDir, MeanWaveDirCardinal, MeanWaveHeight, DomPeriod)
    values (@MeanWaveDir, @MeanWaveDirCard, @MeanWaveHeight, @DomPeriod)
    set @SwellID = SCOPE_IDENTITY()

    -- Should insert all Nulls into tides for now
    insert into dbo.Tide (Incoming, MaximumHeight, MinimumHeight)
    values(@TideIncoming, @TideMaxHeight, @TideMinHeight)
    set @TideID = SCOPE_IDENTITY()

    insert into dbo.Wind (MeanWindDir, MeanWindDirCardinal, MeanWindSpeed, GustSpeed)
    values(@MeanWindDir, @MeanWindDirCard, @MeanWindSpeed, @GustSpeed)
    set @WindID = SCOPE_IDENTITY()

    -- Insert the session last, using the keys of all the other entities created
    insert into dbo.SessionInfo (LocID, TempID, SwellID, TideID, WindID, UserID, SessionDate, SessionTimeIn, SessionTimeOut, Rating)
    VALUES(
        (SELECT LocationID FROM dbo.Location WHERE SpotName = @SpotName),
        @TempID,
        @SwellID,
        @TideID,
        @WindID,
        (SELECT UserID FROM dbo.LogUser WHERE Username = @Username),
        @Date,
        @TimeIn,
        @TimeOut,
        @Rating
    )
END
GO