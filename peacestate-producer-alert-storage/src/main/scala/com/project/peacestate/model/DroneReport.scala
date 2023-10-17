package com.project.peacestate.model

final case class DroneReport (
    droneId: Int, // or String
    latitude: Double,
    longitude: Double,
    citizens: List[Citizen],
    words: List[String]
)