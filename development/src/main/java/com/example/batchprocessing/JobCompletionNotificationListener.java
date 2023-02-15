package com.example.batchprocessing;

import com.example.batchprocessing.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class JobCompletionNotificationListener extends JobExecutionListenerSupport {

    /**
     * This method is used for validation of the batch process only, can be replaced by proper testing
     * @param jobExecution the job execution instance
     */
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
            finally {
                reader.close();
            }
        }
    }
}
