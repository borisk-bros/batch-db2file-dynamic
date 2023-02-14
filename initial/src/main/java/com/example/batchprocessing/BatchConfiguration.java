package com.example.batchprocessing;

import com.example.batchprocessing.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Bean
	public JdbcCursorItemReader<Person> reader(DataSource dataSource) {
		JdbcCursorItemReader<Person> reader = new JdbcCursorItemReader<>();
		reader.setDataSource(dataSource);
		reader.setSql("SELECT first_name, last_name FROM people");
		reader.setRowMapper((rs, i) -> {
			Person person = new Person();
			person.setFirstName(rs.getString(1));
			person.setLastName(rs.getString(2));
			return person;
		});
		return reader;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public FlatFileItemWriter<Person> writer(){
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<>();
		writer.setResource(new FileSystemResource("output.csv"));
		writer.setLineAggregator(new DelimitedLineAggregator<Person>() {{
			setDelimiter(",");
			setFieldExtractor(new BeanWrapperFieldExtractor<Person>() {{
				setNames(new String[] { "firstName", "lastName" });
			}});
		}});

		return writer;
	}

	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(step1)
				.end()
				.build();
	}

	@Bean
	public Step step1(JdbcCursorItemReader<Person> reader) {
		return stepBuilderFactory.get("step1")
				.<Person, Person> chunk(10)
				.reader(reader)
				.processor(processor())
				.writer(writer())
				.build();
	}
}
