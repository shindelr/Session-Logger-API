-- Create LogUser table in dbo
IF OBJECT_ID('dbo.LogUser', 'U') IS NOT NULL
DROP TABLE dbo.LogUser

CREATE TABLE dbo.LogUser
(
    UserID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    Username NVARCHAR(150) NOT NULL,
    Passkey NVARCHAR(150) NOT NULL,
    email NVARCHAR(150) NULL
);

-- Create a new table called 'Loc' in 'dbo'
IF OBJECT_ID('dbo.Location', 'U') IS NOT NULL
DROP TABLE dbo.Location
CREATE TABLE dbo.Location
(
    LocationID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY, 
    SpotName NVARCHAR(200) NOT NULL,
    BuoyNum INT NOT NULL,
    Lat NVARCHAR(15),
    Long NVARCHAR(15)
);

-- Create a new table called 'Temps' in 'dbo'
if OBJECT_ID('dbo.Temps', 'U') IS NOT NULL
Drop TABLE dbo.Temps

CREATE TABLE dbo.Temps
(
    TempID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    AirTemp FLOAT NULL,
    WaterTemp FLOAT NULL,
);

-- Create a new table called 'Swell' in 'dbo'
if OBJECT_ID('dbo.Swell', 'U') IS NOT NULL
Drop TABLE dbo.Swell

CREATE TABLE dbo.Swell
(
    SwellID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    MeanWaveDir INT,
    MeanWaveDirCardinal NVARCHAR(5),  -- Can be up to three characters
    DomPeriod FLOAT,
    MeanWaveHeight FLOAT,
);

-- Create a new table called 'Tide' in 'dbo'
if OBJECT_ID('dbo.Tide', 'U') IS NOT NULL
Drop TABLE dbo.Tide

CREATE TABLE dbo.Tide
(
    TideID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    Incoming BIT NULL,  -- 1 = incoming, 0 = outgoing
    MaximumHeight FLOAT,
    MinimumHeight FLOAT,
);

-- Create a new table called 'Wind' in 'dbo'
if OBJECT_ID('dbo.Wind', 'U') IS NOT NULL
Drop TABLE dbo.Wind

CREATE TABLE dbo.Wind
(
    WindID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    MeanWindDir INT,
    MeanWindDirCardinal NVARCHAR(5),  -- Can be up to three characters
    MeanWindSpeed FLOAT,
    GustSpeed FLOAT,
);

-- Create a new table called 'SessionInfo' in 'dbo'
-- This table will have foreign keys to the other tables
-- SessionInfo is an intersection table
if OBJECT_id('dbo.SessionInfo', 'U') IS NOT NULL
Drop TABLE dbo.SessionInfo

CREATE TABLE dbo.SessionInfo
(
    SessionID INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    LocID INT NOT NULL,
    TempID INT NOT NULL,
    SwellID INT NOT NULL,
    TideID INT NOT NULL,
    WindID INT NOT NULL,
    UserID INT NOT NULL,
    SessionDate DATE NOT NULL,
    SessionTimeIn TIME NOT NULL,
    SessionTimeOut TIME NOT NULL,
    SessionNotes NVARCHAR(500) NULL,
    Rating int,
    FOREIGN KEY (LocID) REFERENCES dbo.Location(LocationID) on delete cascade on update cascade,
    FOREIGN KEY (TempID) REFERENCES dbo.Temps(TempID) on delete cascade on update cascade,
    FOREIGN KEY (SwellID) REFERENCES dbo.Swell(SwellID) on delete cascade on update cascade,
    FOREIGN KEY (TideID) REFERENCES dbo.Tide(TideID) on delete cascade on update cascade,
    FOREIGN KEY (WindID) REFERENCES dbo.Wind(WindID) on delete cascade on update cascade,
    FOREIGN KEY (UserID) REFERENCES dbo.LogUser(UserID) on delete cascade on update cascade
);
