package data.analyzer.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "data")
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Data {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private Long sensorId;
    private LocalDateTime timestamp;
    private double measurement; //չափում

    @Column(name = "type")
    @Enumerated(EnumType.STRING)
    private MeasurementType measurementType;
}
