package com.example.batchprocessing;

import com.example.batchprocessing.model.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

//	private final JdbcTemplate jdbcTemplate;
//
//	@Autowired
//	public JobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
//		this.jdbcTemplate = jdbcTemplate;
//	}
//
//	@Override
//	public void afterJob(JobExecution jobExecution) {
//		if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
//			log.info("!!! JOB FINISHED! Time to verify the results");
//
//			jdbcTemplate.query("SELECT first_name, last_name FROM people",
//				(rs, row) -> new Person(
//					rs.getString(1),
//					rs.getString(2))
//			).forEach(person -> log.info("Found <" + person + "> in the database."));
//		}
//	}

    @Override
    public void afterJob(JobExecution jobExecution) {
        if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");

            // FIXME: 2023-02-14 Rewrite nicely (with streams)

            FlatFileItemReader<Person> reader = new FlatFileItemReaderBuilder<Person>()
                    .name("personItemReader")
                    .resource(new FileSystemResource("output.csv"))
                    .delimited()
                    .names("firstName", "lastName")
                    .fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {{
                        setTargetType(Person.class);
                    }})
                    .build();

            reader.open(jobExecution.getExecutionContext());
            
            try {
                Person person;
                while ((person = reader.read()) != null) {
                    log.info("Found <" + person + "> in the file.");
                }
            } catch(Exception ex) {
                log.error(ex.getMessage());
            }
        }
    }
}
