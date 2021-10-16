using System;

namespace MTFHDataLanding.Infrastructure.Exceptions
{
    public class PersonNotFoundException : EntityNotFoundException
    {
        public PersonNotFoundException(Guid id)
            : base("Person", id)
        { }
    }
}
