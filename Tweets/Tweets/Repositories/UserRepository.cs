using System;
using System.Reflection;
using CorrugatedIron;
using CorrugatedIron.Exceptions;
using CorrugatedIron.Models;
using Tweets.Attributes;
using Tweets.ModelBuilding;
using Tweets.Models;

namespace Tweets.Repositories
{
    public class UserRepository : IUserRepository
    {
        private readonly string bucketName;
        private readonly IRiakClient riakClient;
        private readonly IMapper<User, UserDocument> userDocumentMapper;
        private readonly IMapper<UserDocument, User> userMapper;

        public UserRepository(IRiakClient riakClient, IMapper<User, UserDocument> userDocumentMapper, IMapper<UserDocument, User> userMapper)
        {
            this.riakClient = riakClient;
            this.userDocumentMapper = userDocumentMapper;
            this.userMapper = userMapper;
            bucketName = typeof (UserDocument).GetCustomAttribute<BucketNameAttribute>().BucketName;
        }

        public void Save(User user)
        {
            var userDocument = userDocumentMapper.Map(user);
            var riakObject = new RiakObject(bucketName, userDocument.Id, userDocument);
            var putResult = riakClient.Put(riakObject);
            if (!putResult.IsSuccess)
                throw new Exception(string.Format("Riak returned an error. Code '{0}'. Message: {1}",
                    putResult.ResultCode, putResult.ErrorMessage));
        }

        public User Get(string userName)
        {
            var getResult = riakClient.Get(bucketName, userName);
            if(!getResult.IsSuccess)
                return null;
            return userMapper.Map(getResult.Value.GetObject<UserDocument>());
        }
    }
}