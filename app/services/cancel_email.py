from app.services.missive import send_email

def send_cancellation_email(name, email, order_number, cancel_type="other"):
    subject = "Flight Cancelled due Poor Weather" if cancel_type == "weather" else "Flight Cancelled"

    body = f"""Hello {name},

We’re very sorry to inform you that we unfortunately have to cancel your upcoming scenic flight booking.

This decision is never taken lightly. Aviation operations are heavily dependent on a range of factors such as weather conditions, airspace restrictions, and operational requirements. Unfortunately, on this occasion these factors mean we are unable to operate your scheduled flight safely. As always, the safety of our passengers and crew must remain our absolute top priority.

We completely understand how disappointing this news can be, especially if you have planned your visit to the Whitsundays around this experience. Please accept our sincere apologies for the inconvenience and any disruption this may cause to your travel plans.

The next available flight we currently have available is on 20 March 2026. If this date works for you, we would be very happy to move your booking across to that flight.

If the new date does not suit your schedule, we are of course more than happy to arrange a full refund for your booking.

Please reply to this email and let us know which option you would prefer and we will take care of everything for you as quickly as possible.

Once again, we sincerely apologise for the inconvenience and appreciate your understanding.

Your order number is: {order_number}

Kind Regards,
Ed
Whitsunday Air Tours
"""
    return send_email(email, subject, body)
