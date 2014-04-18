using ServiceStack.Redis;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class UserRepository : IUserRepository
    {
        private readonly RedisClient redisClient;
        private readonly IMapper<User, UserDocument> userDocumentMapper;
        private readonly IMapper<UserDocument, User> userMapper;

        public UserRepository(RedisClient redisClient, IMapper<User, UserDocument> userDocumentMapper, IMapper<UserDocument, User> userMapper)
        {
            this.redisClient = redisClient;
            this.userDocumentMapper = userDocumentMapper;
            this.userMapper = userMapper;
        }

        public void Save(User user)
        {
            var userDocument = userDocumentMapper.Map(user);
            redisClient.Set(userDocument.Id, userDocument);
        }

        public User Get(string userName)
        {
            var userDocument = redisClient.Get<UserDocument>(userName);
            return userDocument == null ? null : userMapper.Map(userDocument);
        }
    }
}