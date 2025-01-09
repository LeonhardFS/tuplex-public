//
// Created by Leonhard Spiegelberg on 12/24/24.
//

#ifndef TUPLEX_CUSTOMARCHIVE_H
#define TUPLEX_CUSTOMARCHIVE_H
// Inspired from cereal binary archive.
#ifdef BUILD_WITH_CEREAL

#include <cereal/cereal.hpp>
#include <sstream>

namespace tuplex {

class BinaryOutputArchive : public cereal::OutputArchive<BinaryOutputArchive, cereal::AllowEmptyClassElision>
    {
    public:
        //! Construct, outputting to the provided stream
        /*! @param stream The stream to output to.  Can be a stringstream, a file stream, or
                          even cout! */
        BinaryOutputArchive(std::ostream & stream) :
                cereal::OutputArchive<BinaryOutputArchive, cereal::AllowEmptyClassElision>(this),
                itsStream(stream)
        { }

        ~BinaryOutputArchive() CEREAL_NOEXCEPT = default;

        //! Writes size bytes of data to the output stream
        void saveBinary( const void * data, std::streamsize size )
        {
            auto const writtenSize = itsStream.rdbuf()->sputn( reinterpret_cast<const char*>( data ), size );

            if(writtenSize != size)
                throw cereal::Exception("Failed to write " + std::to_string(size) + " bytes to output stream! Wrote " + std::to_string(writtenSize));
        }

        void saveType(int hash);

        // Create (closure) of types encoded in this archive.
        std::vector<std::pair<int, std::string>> typeClosure() const;

    private:
        std::ostream & itsStream;


        // (unique) hashes in insert/serialization order.
        std::vector<int> typeHashes;
        std::unordered_map<int, std::string> typeMap;
    };

class BinaryInputArchive : public cereal::InputArchive<BinaryInputArchive, cereal::AllowEmptyClassElision>
    {
    public:
        //! Construct, loading from the provided stream
        BinaryInputArchive(std::istream & stream) :
                InputArchive<BinaryInputArchive, cereal::AllowEmptyClassElision>(this),
                itsStream(stream)
        { }

        ~BinaryInputArchive() CEREAL_NOEXCEPT = default;

        //! Reads size bytes of data from the input stream
        void loadBinary( void * const data, std::streamsize size )
        {
            auto const readSize = itsStream.rdbuf()->sgetn( reinterpret_cast<char*>( data ), size );

            if(readSize != size)
                throw cereal::Exception("Failed to read " + std::to_string(size) + " bytes from input stream! Read " + std::to_string(readSize));
        }

        // Loads type via its hash and performs any registration necessary.
        int loadTypeHash(int encoded_hash);

    private:
        std::istream & itsStream;
    };

    // ######################################################################
    // Common BinaryArchive serialization functions

    //! Saving for POD types to binary
    template<class T> inline
    typename std::enable_if<std::is_arithmetic<T>::value, void>::type
    CEREAL_SAVE_FUNCTION_NAME(BinaryOutputArchive & ar, T const & t)
    {
        ar.saveBinary(std::addressof(t), sizeof(t));
    }

    //! Loading for POD types from binary
    template<class T> inline
    typename std::enable_if<std::is_arithmetic<T>::value, void>::type
    CEREAL_LOAD_FUNCTION_NAME(BinaryInputArchive & ar, T & t)
    {
        ar.loadBinary(std::addressof(t), sizeof(t));
    }

    //! Serializing NVP types to binary
    template <class Archive, class T> inline
    CEREAL_ARCHIVE_RESTRICT(BinaryInputArchive, BinaryOutputArchive)
    CEREAL_SERIALIZE_FUNCTION_NAME( Archive & ar, cereal::NameValuePair<T> & t )
    {
        ar( t.value );
    }

    //! Serializing SizeTags to binary
    template <class Archive, class T> inline
    CEREAL_ARCHIVE_RESTRICT(BinaryInputArchive, BinaryOutputArchive)
    CEREAL_SERIALIZE_FUNCTION_NAME( Archive & ar, cereal::SizeTag<T> & t )
    {
        ar( t.size );
    }

    //! Saving binary data
    template <class T> inline
    void CEREAL_SAVE_FUNCTION_NAME(BinaryOutputArchive & ar, cereal::BinaryData<T> const & bd)
    {
        ar.saveBinary( bd.data, static_cast<std::streamsize>( bd.size ) );
    }

    //! Loading binary data
    template <class T> inline
    void CEREAL_LOAD_FUNCTION_NAME(BinaryInputArchive & ar, cereal::BinaryData<T> & bd)
    {
        ar.loadBinary(bd.data, static_cast<std::streamsize>( bd.size ) );
    }
} // namespace tuplex

// register archives for polymorphic support
CEREAL_REGISTER_ARCHIVE(tuplex::BinaryOutputArchive)
CEREAL_REGISTER_ARCHIVE(tuplex::BinaryInputArchive)

// tie input and output archives together
CEREAL_SETUP_ARCHIVE_TRAITS(tuplex::BinaryInputArchive, tuplex::BinaryOutputArchive)

#endif

#endif //TUPLEX_CUSTOMARCHIVE_H
