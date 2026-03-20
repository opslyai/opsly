def build_cancellation_email(customer_name, order_number):
    return f"""
    <p>Hello {customer_name},</p>

    <p>
    We’re very sorry to inform you that we unfortunately have to cancel your upcoming scenic flight booking.
    </p>

    <p>
    This decision is never taken lightly. Aviation operations are heavily dependent on weather conditions and operational
    requirements, and safety must always remain our top priority. We completely understand how disappointing this news can be,
    especially if you have planned your visit around this experience.
    </p>

    <p>
    At the moment we do not have any availability until <strong>20 March 2026</strong>.
    </p>

    <p>
    If this date works for you we would be very happy to move your booking across to that flight.
    If the new date does not suit your schedule we can also arrange a full refund.
    </p>

    <p>
    Please reply to this email to let us know which option you would prefer.
    </p>

    <p>
    <strong>Your order number is:</strong> {order_number}
    </p>

    <br>

    <p>
    Kind Regards,<br>
    Ed<br>
    Whitsunday Air Tours
    </p>
    """
