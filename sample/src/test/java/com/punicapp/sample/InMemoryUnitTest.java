package com.punicapp.sample;

import com.google.common.base.Optional;
import com.punicapp.rxrepocore.Check;
import com.punicapp.rxrepocore.LocalFilter;
import com.punicapp.rxrepocore.LocalFilters;
import com.punicapp.rxrepoinmemory.InMemoryRepository;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.schedulers.Schedulers;

public class InMemoryUnitTest {
    public static class Cat {
        int id;
        String name;
        int weight; // In kilograms
        Date birthDate;

        @SuppressWarnings("unused")
        public Cat() {
        }

        Cat(int id, String name, int weight, Date birthDate) {
            this.id = id;
            this.name = name;
            this.weight = weight;
            this.birthDate = birthDate;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Cat cat = (Cat) o;

            return id == cat.id;
        }

        @Override
        public int hashCode() {
            return id;
        }
    }

    private InMemoryRepository<Cat> catsRepo;

    @Before
    public void setUp() throws Exception {
        catsRepo = new InMemoryRepository<>(InMemoryRepository.Companion.getSHALLOW_COPY_MODE());
        Observable.fromArray(
                new Cat(1, "Vasya", 12, new Date(1999, 5, 17)),
                new Cat(2, "Alisa", 2, new Date(2005, 2, 4)),
                new Cat(3, "Murka", 4, new Date(2016, 8, 22)),
                new Cat(4, "Qweqwe", 16, new Date(2001, 6, 3)),
                new Cat(5, "Octocat", 22, null),
                new Cat(6, null, 4, null)
        ).doOnNext(catsRepo.saveInChain()).subscribe();
    }

    @Test
    public void testCount() {
        try {
            long count = catsRepo.instantCount(null);
            Assert.assertEquals(count, 6L);
            LocalFilter filter = new LocalFilter("name", Check.IsNotNull, null);
            count = catsRepo.instantCount(new LocalFilters().addFilter(filter));
            Assert.assertEquals(count, 5L);

            InMemoryRepository<Cat> emptyRepo = new InMemoryRepository<>();
            count = emptyRepo.instantCount(null);
            Assert.assertEquals(count, 0L);
        } catch (Exception e) {
            Assert.fail("Exception not handling! " + e.toString());
        }
    }

    @Test
    public void testFetch() {
        try {
            LocalFilter filter = new LocalFilter("weight", Check.Equal, 12);
            Optional<List<Cat>> cats = catsRepo.instantFetch(new LocalFilters().addFilter(filter), null);
            Assert.assertEquals(cats.get().get(0).id, 1);

            // Test null/not null
            filter = new LocalFilter("birthDate", Check.IsNull, null);
            Optional<List<Cat>> catsNullBirth = catsRepo.instantFetch(new LocalFilters().addFilter(filter), null);
            filter = new LocalFilter("birthDate", Check.IsNotNull, null);
            Optional<List<Cat>> catsNotNullBirth = catsRepo.instantFetch(new LocalFilters().addFilter(filter), null);
            boolean removed = new HashSet<>(catsNullBirth.get()).removeAll(catsNotNullBirth.get());
            // Check that catsNullBirth and catsNotNullBirth has no common elements
            Assert.assertFalse(removed);
            Assert.assertEquals((long) (catsNullBirth.get().size() + catsNotNullBirth.get().size()),
                    (long) catsRepo.instantCount(null));

            filter = new LocalFilter("name", Check.In, new String[]{"Alisa", "Qweqwe", "ZZZzzz"});
            cats = catsRepo.instantFetch(new LocalFilters().addFilter(filter), null);
            Assert.assertEquals(cats.get().size(), 2);

            filter = new LocalFilter("birthDate", Check.GreatOrEqual, new Date(2001, 1, 1));
            cats = catsRepo.instantFetch(new LocalFilters().addFilter(filter), null);
            Assert.assertEquals(cats.get().size(), 3);

            // Test immutability of data in storage
            filter = new LocalFilter("name", Check.Equal, "Qweqwe");
            Cat modifiedCat = catsRepo.instantFetch(new LocalFilters().addFilter(filter), null).get().get(0);
            modifiedCat.name = "Qwezzz";

            Cat notModifiedCat = catsRepo.instantFetch(new LocalFilters().addFilter(filter), null).get().get(0);
            Assert.assertNotEquals(modifiedCat.name, notModifiedCat.name);
        } catch (Exception e) {
            Assert.fail("Exception not handling! " + e.toString());
        }
    }

    @Test
    public void testMultithreaded() {
        try {
            InMemoryRepository<Cat> localRepo = new InMemoryRepository<>();

            final int chunkSize = 1024 * 1024;
            int threads = 3;
            List<Observable<Cat>> observables = new ArrayList<>(threads);

            for (int i = 0; i < threads; ++i) {
                final int finalI = i;
                Observable<Cat> observable = Observable.create(new ObservableOnSubscribe<Cat>() {
                    @Override
                    public void subscribe(ObservableEmitter<Cat> observableEmitter) throws Exception {
                        for (int j = 0; j < chunkSize; ++j) {
                            int id = finalI * chunkSize + j;
                            observableEmitter.onNext(new Cat(id, String.valueOf(id),
                                    10, new Date(System.currentTimeMillis())));
                        }
                        observableEmitter.onComplete();
                    }
                }).doOnNext(localRepo.saveInChain())
                        .subscribeOn(Schedulers.newThread());
                observables.add(observable);
            }

            Observable.merge(observables).blockingSubscribe();
            List<Cat> cats = localRepo.instantFetch(null, null).get();

            Assert.assertEquals(cats.size(), threads * chunkSize);
            for (Cat cat : cats) {
                Assert.assertEquals(String.valueOf(cat.id), cat.name);
            }
        } catch (Exception e) {
            Assert.fail("Exception not handling! " + e.toString());
        }
    }
}