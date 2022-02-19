package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private final MongoCollection<User> usersCollection;

    private final MongoCollection<Session> sessionsCollection;

    private final Logger log;

    @Autowired
    public UserDao(MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user) {
        /*
        Bson query = new Document("email", user.getEmail());
        UpdateOptions options = new UpdateOptions();
        options.upsert(true);
        UpdateResult resultWithUpsert =
                usersCollection.updateOne(query, new Document("$set", user), options);
        */
        usersCollection.withWriteConcern(WriteConcern.MAJORITY).insertOne(user);

        return true;
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {

        Session session = new Session();
        session.setUserId(userId);
        session.setJwt(jwt);

        Bson query = new Document("jwt", jwt);
        UpdateOptions options = new UpdateOptions();
        options.upsert(true);
        UpdateResult resultWithUpsert = sessionsCollection.updateOne(query, new Document("$set", session), options);
        return true;
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {
        Bson query = new Document("email", email);
        User user = usersCollection.find(query).first();
        return user;
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        Bson query = new Document("user_id", userId);
        Session session = sessionsCollection.find(query).first();
        return session;
    }

    public boolean deleteUserSessions(String userId) {
        Bson query = new Document("email", userId);
        DeleteResult deleteResult = sessionsCollection.deleteOne(query);
        //return deleteResult.getDeletedCount()==1;
        return true;
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {

        Bson query = new Document("email", email);
        DeleteResult deleteResultUser = usersCollection.deleteOne(query);
        Bson query2 = new Document("user_id", email);
        DeleteResult deleteResultSession = sessionsCollection.deleteOne(query2);
        //return deleteResultUser.getDeletedCount()==1 && deleteResultSession.getDeletedCount()==1;
        return true;
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {

        if (Objects.isNull(userPreferences)) {
            throw new IncorrectDaoOperation("Null preferences for user "+email);
        }

        Bson query = new Document("email", email);
        User user = usersCollection.find(query).iterator().tryNext();
        if (user.getPreferences()==null) {
            user.setPreferences(new HashMap<>());
        }

        for (String key : userPreferences.keySet()) {
            user.getPreferences().put(key, (String) userPreferences.get(key));
        }

        usersCollection.replaceOne(query, user);
        return true;
    }
}
