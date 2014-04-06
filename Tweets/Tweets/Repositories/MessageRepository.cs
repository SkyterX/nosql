using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class MessageRepository : IMessageRepository
    {
        private readonly IMapper<Message, MessageDocument> messageDocumentMapper;
        private readonly MongoCollection<MessageDocument> messagesCollection;

        public MessageRepository(IMapper<Message, MessageDocument> messageDocumentMapper)
        {
            this.messageDocumentMapper = messageDocumentMapper;
            var connectionString = ConfigurationManager.ConnectionStrings["MongoDb"].ConnectionString;
            var databaseName = MongoUrl.Create(connectionString).DatabaseName;
            var mongoDatabase = new MongoClient(connectionString).GetServer().GetDatabase(databaseName);
            messagesCollection =
                mongoDatabase.GetCollection<MessageDocument>(MessageDocument.CollectionName);
        }

        public void Save(Message message)
        {
            var messageDocument = messageDocumentMapper.Map(message);
            messagesCollection.Insert(messageDocument);
        }

        public void Like(Guid messageId, User user)
        {
            var likeDocument = new LikeDocument {UserName = user.Name, CreateDate = DateTime.UtcNow};
            
            var findQuery = Query.And(
                Query<MessageDocument>.EQ(d => d.Id, messageId),
                Query<MessageDocument>.ElemMatch(d => d.Likes, q => q.EQ(like => like.UserName, user.Name)));
            var findResult = messagesCollection.FindOneAs<MessageDocument>(findQuery);
            if (findResult != null)
                return;

            findQuery = Query.And(
                Query<MessageDocument>.EQ(d => d.Id, messageId),
                Query<MessageDocument>.EQ(d => d.Likes, (IEnumerable<LikeDocument>) null));
            findResult = messagesCollection.FindOneAs<MessageDocument>(findQuery);

            var updateQuery = Query<MessageDocument>.EQ(d => d.Id, messageId);
            var update = findResult == null
                ? Update<MessageDocument>.Push(d => d.Likes, likeDocument)
                : Update<MessageDocument>.Set(d => d.Likes, new [] {likeDocument});
            messagesCollection.Update(updateQuery, update);
        }

        public void Dislike(Guid messageId, User user)
        {
            var query = Query<MessageDocument>.EQ(d => d.Id, messageId);
            var update = Update<MessageDocument>.Pull(d => d.Likes,
                builder => builder.EQ(d => d.UserName, user.Name));
            messagesCollection.Update(query, update);
        }

        public IEnumerable<Message> GetPopularMessages()
        {
            var likesIsNull = new BsonDocument {{"$eq", new BsonArray {"$likes", BsonNull.Value}}};
            var likesIsEmpty = new BsonDocument {{"$eq", new BsonArray {"$likes", new BsonArray()}}};
            var hasNoLikes = new BsonDocument {{"$or", new BsonArray {likesIsNull, likesIsEmpty}}};
            var noLikesArray = new BsonArray {BsonNull.Value};
            var likesProjection = new BsonDocument {{"$cond", new BsonArray {hasNoLikes, noLikesArray, "$likes"}}};
            var project = new BsonDocument
            {
                {
                    "$project",
                    new BsonDocument
                    {
                        {"userName", 1},
                        {"text", 1},
                        {"createDate", 1},
                        {"likes", likesProjection}
                    }
                }
            };
            var unwind = new BsonDocument {{"$unwind", "$likes"}};
            var groupId = new BsonDocument
            {
                {"_id", "$_id"},
                {"userName", "$userName"},
                {"text", "$text"},
                {"createDate", "$createDate"}
            };
            var likesCount = new BsonDocument {{"$cond", new BsonArray {likesIsNull, 0, 1}}};
            var likesSum = new BsonDocument {{"$sum", likesCount}};
            var group = new BsonDocument
            {
                {
                    "$group", new BsonDocument
                    {
                        {"_id", groupId},
                        {"likes", likesSum}
                    }
                }
            };
            var sort = new BsonDocument {{"$sort", new BsonDocument {{"likes", -1}}}};
            var limit = new BsonDocument {{"$limit", 10}};

            var result = messagesCollection.Aggregate(project, unwind, group, sort, limit).ResultDocuments;
            foreach (var document in result)
            {
                var messageDocument = BsonSerializer.Deserialize<MessageDocument>((BsonDocument) document["_id"]);
                int likes = (int) document["likes"];
                yield return new Message
                {
                    Id = messageDocument.Id,
                    User = new User {Name = messageDocument.UserName},
                    CreateDate = messageDocument.CreateDate,
                    Text = messageDocument.Text,
                    Likes = likes
                };
            }
        }

        public IEnumerable<UserMessage> GetMessages(User user)
        {
            var query = Query<MessageDocument>.EQ(d => d.UserName, user.Name);
            var findIterator = messagesCollection
                .Find(query)
                .SetSortOrder(SortBy<MessageDocument>.Descending(d => d.CreateDate));
            foreach (var messageDocument in findIterator)
            {
                var likes = messageDocument.Likes == null ? 0 : messageDocument.Likes.Count();
                var liked = messageDocument.Likes != null &&
                            messageDocument.Likes.Any(like => like.UserName == user.Name);
                yield return new UserMessage
                {
                    Id = messageDocument.Id,
                    User = new User {Name = messageDocument.UserName},
                    Text = messageDocument.Text,
                    CreateDate = messageDocument.CreateDate,
                    Likes = likes,
                    Liked = liked
                };
            }
        }
    }
}