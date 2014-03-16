using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.Linq;
using System.Data.Linq.Mapping;
using System.Linq;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class MessageRepository : IMessageRepository
    {
        private readonly string connectionString;
        private readonly AttributeMappingSource mappingSource;
        private readonly IMapper<Message, MessageDocument> messageDocumentMapper;

        public MessageRepository(IMapper<Message, MessageDocument> messageDocumentMapper)
        {
            this.messageDocumentMapper = messageDocumentMapper;
            mappingSource = new AttributeMappingSource();
            connectionString = ConfigurationManager.ConnectionStrings["SqlConnectionString"].ConnectionString;
        }

        public void Save(Message message)
        {
            var messageDocument = messageDocumentMapper.Map(message);
            using (var dataContext = new DataContext(connectionString, mappingSource))
            {
                dataContext
                    .GetTable<MessageDocument>()
                    .InsertOnSubmit(messageDocument);
                dataContext.SubmitChanges();
            }
        }

        public void Like(Guid messageId, User user)
        {
            var likeDocument = new LikeDocument {MessageId = messageId, UserName = user.Name, CreateDate = DateTime.UtcNow};
            using (var dataContext = new DataContext(connectionString, mappingSource))
            {
                dataContext
                    .GetTable<LikeDocument>()
                    .InsertOnSubmit(likeDocument);
                dataContext.SubmitChanges();
            }
        }

        public void Dislike(Guid messageId, User user)
        {
            using (var dataContext = new DataContext(connectionString, mappingSource))
            {
                var likeTable = dataContext.GetTable<LikeDocument>();
                var likeDocument = likeTable
                    .SingleOrDefault(document => document.MessageId == messageId && document.UserName == user.Name);
                if(likeDocument == null) return;
                dataContext
                    .GetTable<LikeDocument>()
                    .DeleteOnSubmit(likeDocument);
                dataContext.SubmitChanges();
            }
        }

        public IEnumerable<Message> GetPopularMessages()
        {
            using (var dataContext = new DataContext(connectionString, mappingSource))
            {
                var messageTable = dataContext.GetTable<MessageDocument>();
                var likeTable = dataContext.GetTable<LikeDocument>();
                var messages = from message in messageTable
                    join like in likeTable 
                          on message.Id equals like.MessageId 
                          into likes
                    select new Message
                    {
                        Id = message.Id,
                        User = new User { Name = message.UserName },
                        Text = message.Text,
                        CreateDate = message.CreateDate,
                        Likes = likes.Count()
                    };
                return messages
                    .OrderByDescending(message => message.Likes)
                    .Take(10)
                    .ToArray();
            }
        }

        public IEnumerable<UserMessage> GetMessages(User user)
        {
            using (var dataContext = new DataContext(connectionString, mappingSource))
            {
                var messageTable = dataContext.GetTable<MessageDocument>();
                var likeTable = dataContext.GetTable<LikeDocument>();
                var userMessages = messageTable
                    .Where(message => message.UserName == user.Name)
                    .OrderByDescending(message => message.CreateDate);
                foreach (var messageDocument in userMessages)
                {
                    var likes = likeTable.Count(like => like.MessageId.Equals(messageDocument.Id));
                    var liked =likeTable.Any(like =>
                                like.MessageId.Equals(messageDocument.Id) &&
                                like.UserName.Equals(messageDocument.UserName));
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
}