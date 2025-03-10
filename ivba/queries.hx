QUERY CancelVenueBooking(venueBookingID: String) =>
    venueBooking <- V<VenueBooking>(venueBookingID)
    l <- venueBooking::UPDATE({Status: "cancelled"})
    DROP venueBooking::InE<HeldAt>
    RETURN NONE