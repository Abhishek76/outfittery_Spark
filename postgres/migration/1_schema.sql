CREATE SCHEMA stack;

CREATE TABLE stack.users (
    Id int,
    Reputation int,
    CreationDate timestamp,
    DisplayName varchar(255),
    WebsiteUrl int,
    LastAccessDate timestamp,
    _Location varchar(255),
    Views int,
    UpVotes int,
    DownVotes int,
    ProfileImageUrl varchar(255),
    AccountId int,
    Last_creationDate timestamp,
    TotalPostsCount int,
    PostsCountLast90Days int,
    is_Editor int,
    is_critic int,
    AverageComment_OverPeriod float,
    AverageValue_OverActiveMonths float
    
);

