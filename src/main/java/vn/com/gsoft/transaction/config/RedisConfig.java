package vn.com.gsoft.transaction.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import vn.com.gsoft.transaction.constant.CachingConstant;

import java.time.Duration;

@Configuration
public class RedisConfig {
    @Value("${redis.host}")
    private String redisHost;

    @Value("${redis.port}")
    private int redisPort;
    @Value("${redis.username}")
    private String username;
    @Value("${redis.password}")
    private String password;
    @Value("${cache.duration.default}")
    private int durationDefault;

    @Value("${cache.duration.user}")
    private int durationUser;

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration configuration = new RedisStandaloneConfiguration(redisHost, redisPort);
        configuration.setUsername(username);
        configuration.setPassword(password);
        LettuceConnectionFactory factory = new LettuceConnectionFactory(configuration);
        factory.setDatabase(CachingConstant.DB_GIAO_DICH_HANG_HOA);
        return factory;
    }

    @Bean
    public RedisCacheManager cacheManager() {
        RedisCacheConfiguration cacheConfig = defaultCacheConfig(Duration.ofMinutes(durationDefault)).disableCachingNullValues();

        return RedisCacheManager.builder(redisConnectionFactory())
                .cacheDefaults(cacheConfig)
                .withCacheConfiguration(CachingConstant.USER, defaultCacheConfig(Duration.ofMinutes(durationUser)))
                .build();
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);

        // Serializer cho key là String
        template.setKeySerializer(new StringRedisSerializer());

        // Serializer cho value là đối tượng GiaoDichHangHoa
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());

        return template;
    }

    private RedisCacheConfiguration defaultCacheConfig(Duration duration) {
        return RedisCacheConfiguration
                .defaultCacheConfig()
                .entryTtl(duration)
                .serializeValuesWith(RedisSerializationContext.SerializationPair.fromSerializer(new GenericJackson2JsonRedisSerializer()));
    }
}
