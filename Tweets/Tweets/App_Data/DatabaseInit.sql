create Table Messages (
	[Id] uniqueidentifier
		constraint PK_Messages
		primary key
		not null,
	[UserName] varchar(100),
	[Text] varchar(1000),
	[CreateDate] datetime,
	[Version] rowversion
		not null
)

go

create Table Likes (
	[UserName] varchar(100),
	[MessageId] uniqueidentifier
		constraint FK_Likes_To_Messages
		foreign key references Messages(Id)
		not null,
	[CreateDate] datetime,
	constraint PK_Likes
		primary key(UserName, MessageId)
)

go